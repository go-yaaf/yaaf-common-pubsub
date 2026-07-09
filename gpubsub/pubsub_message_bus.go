package gpubsub

import (
	"context"
	"fmt"
	"time"

	"github.com/go-yaaf/yaaf-common/config"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-yaaf/yaaf-common/logger"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

// region Message Bus actions ------------------------------------------------------------------------------------------

// Publish sends messages to a Google Cloud Pub/Sub topic.
// It ensures that the topic for each message exists, creating it if necessary.
// Messages are published in batches based on their topic.
func (r *pubSubAdapter) Publish(messages ...IMessage) error {
	if len(messages) == 0 {
		return nil
	}

	// A map to hold publishers per topic, to avoid creating them multiple times.
	publishersMap := make(map[string]*pubsub.Publisher)
	for _, message := range messages {
		if _, ok := publishersMap[message.Topic()]; !ok {
			topicName, err := r.getOrCreateTopic(message.Topic())
			if err != nil {
				return fmt.Errorf("failed to get or create topic '%s': %v", message.Topic(), err)
			}
			publishersMap[message.Topic()] = r.newPublisher(topicName)
		}
	}

	// Stop the publishers once done to flush and release their background resources.
	defer func() {
		for _, publisher := range publishersMap {
			publisher.Stop()
		}
	}()

	// Publish each message to its corresponding topic.
	for _, message := range messages {
		bytes, err := messageToRaw(message)
		if err != nil {
			return fmt.Errorf("failed to serialize message: %v", err)
		}

		if publisher, ok := publishersMap[message.Topic()]; ok {
			result := publisher.Publish(context.Background(), &pubsub.Message{Data: bytes})
			if _, err := result.Get(context.Background()); err != nil {
				return fmt.Errorf("failed to publish message to topic '%s': %v", message.Topic(), err)
			}
		}
	}
	return nil
}

// Subscribe creates a subscription to a Google Cloud Pub/Sub topic and starts listening for messages.
// This implementation only supports subscribing to a single topic at a time.
// A new subscription is created if one with the given name doesn't already exist.
// The callback function is invoked for each received message.
func (r *pubSubAdapter) Subscribe(subscriberName string, factory MessageFactory, callback SubscriptionCallback, topics ...string) (string, error) {
	if len(topics) != 1 {
		return "", fmt.Errorf("only one topic is allowed in this implementation")
	}

	topicName, err := r.getOrCreateTopic(topics[0])
	if err != nil {
		return "", err
	}

	sub, err := r.getOrCreateSubscription(topicName, subscriberName)
	if err != nil {
		return "", err
	}

	// receiver is the function that will be called for each message.
	receiver := func(ctx context.Context, m *pubsub.Message) {
		msg, err := rawToMessage(factory, m.Data)
		if err != nil {
			// Drop the malformed message (Ack to prevent redelivery), but log loudly with the
			// message id so the drop is visible and can be alerted on.
			m.Ack()
			logger.Error("Dropping malformed message id=%s on subscription %s: %v", m.ID, subscriberName, err)
			return
		}
		// Process the message and Acknowledge or Nacknowledge based on the callback result.
		if callback(msg) {
			m.Ack()
		} else {
			m.Nack()
		}
	}

	// Create a cancellable context so the receive loop can be stopped by Unsubscribe or Close.
	ctx, cancel := context.WithCancel(context.Background())
	sId := sub.ID()
	handle := &subReceiver{cancel: cancel}

	r.mu.Lock()
	// Cancel any pre-existing loop registered under the same id to avoid leaking it.
	if prev, ok := r.cancels[sId]; ok {
		prev.cancel()
	}
	r.cancels[sId] = handle
	r.mu.Unlock()

	// Start receiving messages in a background goroutine.
	go func() {
		logger.Info("Starting subscription %s for topic %s", sId, topics[0])
		if err := sub.Receive(ctx, receiver); err != nil && ctx.Err() == nil {
			logger.Error("Subscription %s receive error: %v", sId, err)
		}
		logger.Warn("Subscription %s has ended", sId)

		// De-register this loop (only if it is still the current registration).
		r.mu.Lock()
		if r.cancels[sId] == handle {
			delete(r.cancels, sId)
		}
		r.mu.Unlock()
		cancel()
	}()

	return sId, nil
}

// Unsubscribe stops the local receive loop and deletes the subscription from Google Cloud Pub/Sub.
func (r *pubSubAdapter) Unsubscribe(subscriptionId string) bool {
	if err := validateResourceName("subscription", subscriptionId); err != nil {
		logger.Error("Failed to unsubscribe: %v", err)
		return false
	}

	// Stop the receive goroutine first so it does not keep pulling from a subscription
	// that is about to be deleted.
	r.mu.Lock()
	if handle, ok := r.cancels[subscriptionId]; ok {
		handle.cancel()
		delete(r.cancels, subscriptionId)
	}
	r.mu.Unlock()

	err := r.client.SubscriptionAdminClient.DeleteSubscription(context.Background(), &pubsubpb.DeleteSubscriptionRequest{
		Subscription: r.subscriptionPath(subscriptionId),
	})
	if err != nil {
		logger.Error("Failed to unsubscribe %s: %v", subscriptionId, err)
		return false
	}
	return true
}

// Push is not supported in the Google Pub/Sub implementation. Use Publish instead.
func (r *pubSubAdapter) Push(messages ...IMessage) error {
	return fmt.Errorf("push is not supported in Google pubsub implementation, use Producer.Publish()")
}

// Pop is not supported in the Google Pub/Sub implementation. Use a Consumer to read messages.
func (r *pubSubAdapter) Pop(factory MessageFactory, timeout time.Duration, queue ...string) (IMessage, error) {
	return nil, fmt.Errorf("pop is not supported in Google pubsub implementation, use Consumer.Subscribe()")
}

// CreateProducer creates a message producer for a specific topic.
func (r *pubSubAdapter) CreateProducer(topicName string) (IMessageProducer, error) {
	topicPath, err := r.getOrCreateTopic(topicName)
	if err != nil {
		return nil, err
	}
	return &pubSubProducer{topicName: topicName, publisher: r.newPublisher(topicPath)}, nil
}

// CreateConsumer creates a message consumer for a specific topic.
// This implementation only supports consuming from a single topic.
func (r *pubSubAdapter) CreateConsumer(subscriberName string, mf MessageFactory, topics ...string) (IMessageConsumer, error) {
	if len(topics) != 1 {
		return nil, fmt.Errorf("only one topic is allowed in this implementation")
	}

	topicName, err := r.getOrCreateTopic(topics[0])
	if err != nil {
		return nil, err
	}

	sub, err := r.getOrCreateSubscription(topicName, subscriberName)
	if err != nil {
		return nil, err
	}

	return &pubSubConsumer{topicName: topics[0], subscriber: sub, factory: mf}, nil
}

// endregion

// region Producer actions ---------------------------------------------------------------------------------------------

// pubSubProducer is an implementation of IMessageProducer for Google Cloud Pub/Sub.
type pubSubProducer struct {
	topicName string
	publisher *pubsub.Publisher
}

// Close stops the underlying publisher, flushing any buffered messages and releasing its resources.
func (p *pubSubProducer) Close() error {
	if p.publisher != nil {
		p.publisher.Stop()
	}
	return nil
}

// Publish sends messages to the producer's topic.
// It only publishes messages that match the producer's topic name.
// If message ordering is enabled, it uses the message's addressee as the ordering key.
func (p *pubSubProducer) Publish(messages ...IMessage) error {
	if len(messages) == 0 {
		return nil
	}

	if p.publisher == nil {
		return fmt.Errorf("topic: %s not initialized", p.topicName)
	}

	for _, message := range messages {
		if message.Topic() == p.topicName {
			bytes, err := messageToRaw(message)
			if err != nil {
				return err
			}

			msg := &pubsub.Message{Data: bytes}
			if p.publisher.EnableMessageOrdering && message.Addressee() != "" {
				msg.OrderingKey = message.Addressee()
			}

			result := p.publisher.Publish(context.Background(), msg)
			if _, err := result.Get(context.Background()); err != nil {
				return err
			}
		}
	}
	return nil
}

// endregion

// region Consumer actions ---------------------------------------------------------------------------------------------

// pubSubConsumer is an implementation of IMessageConsumer for Google Cloud Pub/Sub.
type pubSubConsumer struct {
	topicName  string
	subscriber *pubsub.Subscriber
	factory    MessageFactory
}

// Close does nothing in this implementation, as the underlying subscription is managed by the adapter.
func (c *pubSubConsumer) Close() error {
	return nil
}

// Read blocks until a new message arrives or until the timeout expires.
// Use a timeout of 0 for unlimited blocking.
//
// Example of usage in an infinite loop:
//
//	for {
//		if msg, err := consumer.Read(time.Second * 5); err != nil {
//			// Handle error (e.g., timeout)
//		} else {
//			// Process message in a dedicated goroutine
//			go processThisMessage(msg)
//		}
//	}
func (c *pubSubConsumer) Read(timeout time.Duration) (IMessage, error) {
	ctx, cancel := context.WithCancel(context.Background())
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	defer cancel()

	type readResult struct {
		msg IMessage
		err error
	}
	// Buffered (size 1) so the receive callback hands off the first message without
	// blocking and without sharing locals across the callback goroutine boundary.
	resultCh := make(chan readResult, 1)

	recvErr := c.subscriber.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		msg, err := rawToMessage(c.factory, m.Data)
		select {
		case resultCh <- readResult{msg: msg, err: err}:
			// This is the message we will return to the caller.
			m.Ack()
			cancel() // Stop receiving after the first message.
		default:
			// A message was already captured (Receive may prefetch several concurrently);
			// Nack the extras so Pub/Sub redelivers them instead of dropping them.
			m.Nack()
		}
	})

	// A delivered message (or its parse error) always takes precedence over the
	// Receive return value, which is typically context.Canceled once we stop.
	select {
	case res := <-resultCh:
		return res.msg, res.err
	default:
	}

	if recvErr != nil && recvErr != context.Canceled {
		return nil, recvErr
	}
	return nil, fmt.Errorf("read timeout")
}

// endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// newPublisher creates a Pub/Sub publisher for the given (fully qualified) topic name,
// enabling message ordering on the client side when configured.
func (r *pubSubAdapter) newPublisher(topicName string) *pubsub.Publisher {
	publisher := r.client.Publisher(topicName)
	if config.Get().EnableMessageOrdering() {
		publisher.EnableMessageOrdering = true
	}
	return publisher
}

// getOrCreateTopic retrieves an existing Pub/Sub topic or creates it if it doesn't exist.
// It returns the fully qualified topic name (projects/{project}/topics/{topic}).
func (r *pubSubAdapter) getOrCreateTopic(topicName string) (string, error) {
	if err := validateResourceName("topic", topicName); err != nil {
		return "", err
	}
	topicPath := r.topicPath(topicName)

	// In v2 the Exists() helper is gone; probe with a Get and treat NotFound as "create it".
	_, err := r.client.TopicAdminClient.GetTopic(context.Background(), &pubsubpb.GetTopicRequest{Topic: topicPath})
	if err == nil {
		return topicPath, nil
	}
	if status.Code(err) != codes.NotFound {
		return "", fmt.Errorf("failed to check if topic %s exists: %v", topicName, err)
	}

	// The topic is missing: only create it when implicit creation is enabled.
	if !autoCreateResources() {
		return "", fmt.Errorf("topic %s does not exist and auto-creation is disabled (%s=false)", topicName, cfgAutoCreateResources)
	}

	if _, err = r.client.TopicAdminClient.CreateTopic(context.Background(), &pubsubpb.Topic{Name: topicPath}); err != nil {
		return "", fmt.Errorf("failed to create topic %s: %v", topicName, err)
	}
	return topicPath, nil
}

// getOrCreateSubscription retrieves an existing subscription or creates a new one for a given topic.
// The topicName parameter must be the fully qualified topic name.
func (r *pubSubAdapter) getOrCreateSubscription(topicName string, subscriberName string) (*pubsub.Subscriber, error) {
	if err := validateResourceName("subscription", subscriberName); err != nil {
		return nil, err
	}
	cfg := config.Get()
	subscriptionPath := r.subscriptionPath(subscriberName)

	// Probe with a Get; a NotFound status means we need to create the subscription.
	_, err := r.client.SubscriptionAdminClient.GetSubscription(context.Background(), &pubsubpb.GetSubscriptionRequest{Subscription: subscriptionPath})
	if err != nil {
		if status.Code(err) != codes.NotFound {
			return nil, fmt.Errorf("failed to check if subscription %s exists: %v", subscriberName, err)
		}

		// The subscription is missing: only create it when implicit creation is enabled.
		if !autoCreateResources() {
			return nil, fmt.Errorf("subscription %s does not exist and auto-creation is disabled (%s=false)", subscriberName, cfgAutoCreateResources)
		}

		_, err = r.client.SubscriptionAdminClient.CreateSubscription(context.Background(), &pubsubpb.Subscription{
			Name:                  subscriptionPath,
			Topic:                 topicName,
			AckDeadlineSeconds:    int32(cfg.PubSubAckDeadline()),
			EnableMessageOrdering: cfg.EnableMessageOrdering(),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create subscription %s: %v", subscriberName, err)
		}
	}

	// Build the subscriber handle and apply receive settings from the application config.
	sub := r.client.Subscriber(subscriptionPath)
	sub.ReceiveSettings.NumGoroutines = cfg.PubSubNumOfGoroutines()
	sub.ReceiveSettings.MaxOutstandingMessages = cfg.PubSubMaxOutstandingMessages()
	sub.ReceiveSettings.MaxOutstandingBytes = cfg.PubSubMaxOutstandingBytes()

	return sub, nil
}

// endregion
