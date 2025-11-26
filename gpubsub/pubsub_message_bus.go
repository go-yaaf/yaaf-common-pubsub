package gpubsub

import (
	"context"
	"fmt"
	"time"

	"github.com/go-yaaf/yaaf-common/config"

	"cloud.google.com/go/pubsub"

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

	// A map to hold topic objects, to avoid creating them multiple times.
	topicsMap := make(map[string]*pubsub.Topic)
	for _, message := range messages {
		if _, ok := topicsMap[message.Topic()]; !ok {
			topic, err := r.getOrCreateTopic(message.Topic())
			if err != nil {
				return fmt.Errorf("failed to get or create topic '%s': %v", message.Topic(), err)
			}
			topicsMap[message.Topic()] = topic
		}
	}

	// Publish each message to its corresponding topic.
	for _, message := range messages {
		bytes, err := messageToRaw(message)
		if err != nil {
			return fmt.Errorf("failed to serialize message: %v", err)
		}

		if topic, ok := topicsMap[message.Topic()]; ok {
			result := topic.Publish(context.Background(), &pubsub.Message{Data: bytes})
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

	topic, err := r.getOrCreateTopic(topics[0])
	if err != nil {
		return "", err
	}

	sub, err := r.getOrCreateSubscription(topic, subscriberName)
	if err != nil {
		return "", err
	}

	// receiver is the function that will be called for each message.
	receiver := func(ctx context.Context, m *pubsub.Message) {
		msg, err := rawToMessage(factory, m.Data)
		if err != nil {
			// Acknowledge the message to prevent redelivery if it's malformed.
			m.Ack()
			logger.Error("Failed to convert raw data to message: %v", err)
		} else {
			// Process the message and Acknowledge or Nacknowledge based on the callback result.
			if callback(msg) {
				m.Ack()
			} else {
				m.Nack()
			}
		}
	}

	// Start receiving messages in a background goroutine.
	go func() {
		ctx := context.Background()
		sId := sub.ID()
		logger.Info("Starting subscription %s for topic %s", sId, topics[0])
		if err := sub.Receive(ctx, receiver); err != nil {
			logger.Error("Subscription %s receive error: %v", sId, err)
		}
		logger.Warn("Subscription %s has ended", sId)
	}()

	return sub.ID(), nil
}

// Unsubscribe deletes a subscription from Google Cloud Pub/Sub.
func (r *pubSubAdapter) Unsubscribe(subscriptionId string) bool {
	err := r.client.Subscription(subscriptionId).Delete(context.Background())
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
	topic, err := r.getOrCreateTopic(topicName)
	if err != nil {
		return nil, err
	}
	return &pubSubProducer{topicName: topicName, topic: topic}, nil
}

// CreateConsumer creates a message consumer for a specific topic.
// This implementation only supports consuming from a single topic.
func (r *pubSubAdapter) CreateConsumer(subscriberName string, mf MessageFactory, topics ...string) (IMessageConsumer, error) {
	if len(topics) != 1 {
		return nil, fmt.Errorf("only one topic is allowed in this implementation")
	}

	topic, err := r.getOrCreateTopic(topics[0])
	if err != nil {
		return nil, err
	}

	sub, err := r.getOrCreateSubscription(topic, subscriberName)
	if err != nil {
		return nil, err
	}

	return &pubSubConsumer{topicName: topics[0], subscription: sub, factory: mf}, nil
}

// endregion

// region Producer actions ---------------------------------------------------------------------------------------------

// pubSubProducer is an implementation of IMessageProducer for Google Cloud Pub/Sub.
type pubSubProducer struct {
	topicName string
	topic     *pubsub.Topic
}

// Close does nothing in this implementation, as the underlying topic is managed by the adapter.
func (p *pubSubProducer) Close() error {
	return nil
}

// Publish sends messages to the producer's topic.
// It only publishes messages that match the producer's topic name.
// If message ordering is enabled, it uses the message's addressee as the ordering key.
func (p *pubSubProducer) Publish(messages ...IMessage) error {
	if len(messages) == 0 {
		return nil
	}

	if p.topic == nil {
		return fmt.Errorf("topic: %s not initialized", p.topicName)
	}

	for _, message := range messages {
		if message.Topic() == p.topicName {
			bytes, err := messageToRaw(message)
			if err != nil {
				return err
			}

			msg := &pubsub.Message{Data: bytes}
			if p.topic.EnableMessageOrdering && message.Addressee() != "" {
				msg.OrderingKey = message.Addressee()
			}

			result := p.topic.Publish(context.Background(), msg)
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
	topicName    string
	subscription *pubsub.Subscription
	factory      MessageFactory
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
	var message IMessage
	var err error

	ctx, cancel := context.WithCancel(context.Background())
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}
	defer cancel()

	// Receive one message.
	err = c.subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		message, err = rawToMessage(c.factory, m.Data)
		m.Ack()
		cancel() // Cancel the context to stop receiving after one message.
	})

	if err != nil && err != context.Canceled {
		return nil, err
	}

	if message == nil {
		return nil, fmt.Errorf("read timeout")
	}

	return message, err
}

// endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// getOrCreateTopic retrieves an existing Pub/Sub topic or creates it if it doesn't exist.
func (r *pubSubAdapter) getOrCreateTopic(topicName string) (*pubsub.Topic, error) {
	topic := r.client.Topic(topicName)

	exists, err := topic.Exists(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to check if topic %s exists: %v", topicName, err)
	}

	if !exists {
		topic, err = r.client.CreateTopic(context.Background(), topicName)
		if err != nil {
			return nil, fmt.Errorf("failed to create topic %s: %v", topicName, err)
		}
	}

	// Configure message ordering if enabled in the application config.
	if config.Get().EnableMessageOrdering() {
		topic.EnableMessageOrdering = true
	}
	return topic, nil
}

// getOrCreateSubscription retrieves an existing subscription or creates a new one for a given topic.
func (r *pubSubAdapter) getOrCreateSubscription(topic *pubsub.Topic, subscriberName string) (*pubsub.Subscription, error) {
	cfg := config.Get()
	sub := r.client.Subscription(subscriberName)

	exists, err := sub.Exists(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to check if subscription %s exists: %v", subscriberName, err)
	}

	if !exists {
		sub, err = r.client.CreateSubscription(context.Background(), subscriberName,
			pubsub.SubscriptionConfig{
				Topic:                 topic,
				AckDeadline:           time.Second * time.Duration(cfg.PubSubAckDeadline()),
				EnableMessageOrdering: cfg.EnableMessageOrdering(),
			})
		if err != nil {
			return nil, fmt.Errorf("failed to create subscription %s: %v", subscriberName, err)
		}
	}

	// Apply receive settings from the application config.
	sub.ReceiveSettings.NumGoroutines = cfg.PubSubNumOfGoroutines()
	sub.ReceiveSettings.MaxOutstandingMessages = cfg.PubSubMaxOutstandingMessages()
	sub.ReceiveSettings.MaxOutstandingBytes = cfg.PubSubMaxOutstandingBytes()

	return sub, nil
}

// endregion
