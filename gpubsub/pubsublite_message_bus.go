package gpubsub

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-yaaf/yaaf-common/config"
	"github.com/googleapis/gax-go/v2/apierror"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsublite"
	"cloud.google.com/go/pubsublite/pscompat"

	. "github.com/go-yaaf/yaaf-common/messaging"
)

// region Message Bus actions ------------------------------------------------------------------------------------------

// Publish messages to a channel (topic)
func (r *pubSubLiteAdapter) Publish(messages ...IMessage) error {

	if len(messages) == 0 {
		return nil
	}

	// Create publishers map
	publishersMap := make(map[string]*pscompat.PublisherClient)

	for _, message := range messages {
		if message == nil {
			continue
		}
		if _, ok := publishersMap[message.Topic()]; !ok {
			if topic, err := r.getOrCreateTopic(message.Topic()); err != nil {
				return err
			} else {
				if publisher, er := pscompat.NewPublisherClient(context.Background(), topic.Name); er != nil {
					return er
				} else {
					publishersMap[message.Topic()] = publisher
				}
			}
		}
	}

	// Send messages to topic
	for _, message := range messages {
		if bytes, er := messageToRaw(message); er != nil {
			return er
		} else {
			if publisher, ok := publishersMap[message.Topic()]; ok {
				publisher.Publish(context.Background(), &pubsub.Message{Data: bytes})
			}
		}
	}

	// Stop all publishers
	for _, pub := range publishersMap {
		pub.Stop()
	}
	return nil
}

// Subscribe on topics
func (r *pubSubLiteAdapter) Subscribe(subscriberName string, factory MessageFactory, callback SubscriptionCallback, topics ...string) (subscriptionId string, error error) {

	if len(topics) > 1 {
		return "", fmt.Errorf("only one topic allowed in this implementation")
	}

	_, err := r.getOrCreateSubscription(topics[0], subscriberName)

	if err != nil {
		return "", err
	}

	settings := pscompat.ReceiveSettings{
		MaxOutstandingMessages: config.Get().PubSubMaxOutstandingMessages(),
		MaxOutstandingBytes:    config.Get().PubSubMaxOutstandingBytes(),
	}
	subPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", r.gcpProject, r.gcpLocation, subscriberName)
	subscriber, err := pscompat.NewSubscriberClientWithSettings(context.Background(), subPath, settings)

	if err != nil {
		return "", err
	}

	go func() {
		r.readMessages(subscriber, factory, callback)
	}()
	return subPath, nil
}

// Main loop of reading messages from the topic
func (r *pubSubLiteAdapter) readMessages(subscriber *pscompat.SubscriberClient, factory MessageFactory, callback SubscriptionCallback) {

	var err error
	ctx := context.Background()
	for {
		cCtx, cancel := context.WithCancel(ctx)
		err = subscriber.Receive(cCtx, func(ctx context.Context, m *pubsub.Message) {
			if msg, er := rawToMessage(factory, m.Data); er != nil {
				m.Nack()
			} else {
				if callback(msg) {
					m.Ack()
				} else {
					m.Nack()
				}
			}
		})
		if err != nil {
			if errors.Is(err, pscompat.ErrBackendUnavailable) {
				// TODO: Alert if necessary. Receive can be retried.
				continue
			} else {
				// TODO: Handle fatal error.
				cancel()
				break
			}
		}

		// Call cancel from the receiver callback or another goroutine to stop receiving.
		cancel()
	}
}

// Unsubscribe with the given subscriber id
func (r *pubSubLiteAdapter) Unsubscribe(subscriptionId string) bool {
	err := r.client.DeleteSubscription(context.Background(), subscriptionId)
	return err != nil
}

// Push Append one or multiple messages to a queue
func (r *pubSubLiteAdapter) Push(messages ...IMessage) error {
	return fmt.Errorf("push is not supported in Google pubsub lite implementation, use Producer.Publish()")
}

// Pop Remove and get the last message in a queue or block until timeout expires
func (r *pubSubLiteAdapter) Pop(factory MessageFactory, timeout time.Duration, queue ...string) (IMessage, error) {
	return nil, fmt.Errorf("pop is not supported in Google pubsub lite implementation, use Consumer.Subscribe()")
}

// CreateProducer creates message producer for specific topic
func (r *pubSubLiteAdapter) CreateProducer(topicName string) (IMessageProducer, error) {
	if topic, err := r.getOrCreateTopic(topicName); err != nil {
		return nil, err
	} else {
		if publisher, er := pscompat.NewPublisherClient(context.Background(), topic.Name); er != nil {
			return nil, err
		} else {
			return &pubSubLiteProducer{topicName: topicName, publisher: publisher}, nil
		}
	}
}

// CreateConsumer creates message consumer for a specific topic
func (r *pubSubLiteAdapter) CreateConsumer(subscriberName string, mf MessageFactory, topics ...string) (IMessageConsumer, error) {
	if len(topics) != 1 {
		return nil, fmt.Errorf("only one topic allawed in this implementation")
	}

	// Ensure topic exists
	_, fe := r.getOrCreateTopic(topics[0])
	if fe != nil {
		return nil, fe
	}

	// Ensure subscription exists
	_, err := r.getOrCreateSubscription(topics[0], subscriberName)
	if err != nil {
		return nil, err
	}

	// Create subscriber client
	subPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", r.gcpProject, r.gcpLocation, subscriberName)
	subscriber, err := pscompat.NewSubscriberClient(context.Background(), subPath)
	if err != nil {
		return nil, err
	}

	return &pubLiteSubConsumer{topicName: topics[0], subscriber: subscriber, factory: mf}, nil
}

// endregion

// region Producer actions ---------------------------------------------------------------------------------------------

type pubSubLiteProducer struct {
	topicName string
	publisher *pscompat.PublisherClient
}

// Close producer does nothing in this implementation
func (p *pubSubLiteProducer) Close() error {
	p.publisher.Stop()
	return nil
}

// Publish messages to a producer channel (topic)
func (p *pubSubLiteProducer) Publish(messages ...IMessage) error {

	if len(messages) == 0 {
		return nil
	}

	if p.publisher == nil {
		return fmt.Errorf("publisher for topic: %s not initialized", p.topicName)
	}

	// Send messages to topic (only messages wit the same topic name)
	for _, message := range messages {
		if message == nil {
			continue
		}
		if bytes, er := messageToRaw(message); er != nil {
			return er
		} else {
			if message.Topic() == p.topicName {
				p.publisher.Publish(context.Background(), &pubsub.Message{Data: bytes})
			}
		}
	}
	return nil
}

// region Consumer actions ---------------------------------------------------------------------------------------------

type pubLiteSubConsumer struct {
	topicName  string
	subscriber *pscompat.SubscriberClient
	factory    MessageFactory
}

// Close producer does nothing in this implementation
func (c *pubLiteSubConsumer) Close() error {
	return nil
}

// Read message from topic, blocks until a new message arrive or until timeout
func (c *pubLiteSubConsumer) Read(timeout time.Duration) (message IMessage, err error) {

	ctx, cancel := context.WithCancel(context.Background())
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}

	er := c.subscriber.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		message, err = rawToMessage(c.factory, m.Data)
		m.Ack()
		cancel()
	})

	if er == nil || er == context.Canceled {
		if message == nil {
			return nil, fmt.Errorf("read timeout")
		} else {
			return message, err
		}
	} else {
		return nil, er
	}
}

// endregion

// endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// Get topic or create it if not exists
func (r *pubSubLiteAdapter) getOrCreateTopic(topicName string) (*pubsublite.TopicConfig, error) {

	ctx := context.Background()
	topicPath := fmt.Sprintf("projects/%s/locations/%s/topics/%s", r.gcpProject, r.gcpLocation, topicName)

	if topic, err := r.client.Topic(ctx, topicPath); err == nil {
		return topic, nil
	}

	topicConfig := pubsublite.TopicConfig{
		Name:                       topicPath,
		PartitionCount:             1,
		PublishCapacityMiBPerSec:   4,
		SubscribeCapacityMiBPerSec: 4,
		PerPartitionBytes:          30 * 1024 * 1024 * 1024, // 30 GiB
		RetentionDuration:          pubsublite.InfiniteRetention,
	}

	return r.client.CreateTopic(ctx, topicConfig)
}

// Get reference to existing subscription or create new topic if not exists
func (r *pubSubLiteAdapter) getOrCreateSubscription(topicName, subscriberName string) (*pubsublite.SubscriptionConfig, error) {

	var (
		apiErr *apierror.APIError
	)
	ctx := context.Background()
	topicPath := fmt.Sprintf("projects/%s/locations/%s/topics/%s", r.gcpProject, r.gcpLocation, topicName)
	subscriptionPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", r.gcpProject, r.gcpLocation, subscriberName)

	subConfigRequested := pubsublite.SubscriptionConfig{
		Name:                subscriptionPath,
		Topic:               topicPath,
		DeliveryRequirement: pubsublite.DeliverImmediately,
	}

	subConfigReturned, err := r.client.CreateSubscription(ctx, subConfigRequested)
	if err != nil {
		if errors.As(err, &apiErr) {
			if apiErr.Details().ErrorInfo.Reason == "RESOURCE_ALREADY_EXISTS" {
				// try to swallow an error
				err = nil
			}
		}
	}
	return subConfigReturned, err
}

// endregion
