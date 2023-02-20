package gpubsub

import (
	"context"
	"fmt"
	"github.com/go-yaaf/yaaf-common/logger"
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
func (r *pubSubLiteAdapter) Subscribe(factory MessageFactory, callback SubscriptionCallback, subscriberName string, topics ...string) (subscriptionId string, error error) {

	if len(topics) != 1 {
		return "", fmt.Errorf("only one topic allawed in this implementation")
	}

	_, err := r.getOrCreateSubscription(topics[0], subscriberName)
	if err != nil {
		return "", err
	}

	subPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", r.gcpProject, r.gcpZone, subscriberName)
	subscriber, err := pscompat.NewSubscriberClient(context.Background(), subPath)
	if err != nil {
		return "", err
	}

	receiver := func(ctx context.Context, m *pubsub.Message) {
		if msg, er := rawToMessage(factory, m.Data); er != nil {
			m.Ack()
		} else {
			if callback(msg) {
				m.Ack()
			} else {
				m.Nack()
			}
		}
	}
	go func() {
		logger.Error("Subscribe error: %z", subscriber.Receive(context.Background(), receiver))
	}()
	return subPath, nil
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

// endregion

// region Producer actions ---------------------------------------------------------------------------------------------

type pubSubLiteProducer struct {
	topicName string
	publisher *pscompat.PublisherClient
}

// Close producer does noting in this implementation
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

// endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// Get topic or create it if not exists
func (r *pubSubLiteAdapter) getOrCreateTopic(topicName string) (*pubsublite.TopicConfig, error) {

	ctx := context.Background()
	topicPath := fmt.Sprintf("projects/%s/locations/%s/topics/%s", r.gcpProject, r.gcpZone, topicName)

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

	ctx := context.Background()
	topicPath := fmt.Sprintf("projects/%s/locations/%s/topics/%s", r.gcpProject, r.gcpZone, topicName)
	subPath := fmt.Sprintf("projects/%s/locations/%s/subscriptions/%s", r.gcpProject, r.gcpZone, subscriberName)

	subConfig := pubsublite.SubscriptionConfig{
		Name:                subPath,
		Topic:               topicPath,
		DeliveryRequirement: pubsublite.DeliverImmediately,
	}
	return r.client.CreateSubscription(ctx, subConfig)
}

// endregion
