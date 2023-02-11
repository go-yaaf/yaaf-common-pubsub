package gpubsub

import (
	"context"
	"fmt"
	"time"

	_ "encoding/json"
	_ "github.com/google/uuid"

	"cloud.google.com/go/pubsub"

	_ "github.com/go-yaaf/yaaf-common/logger"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

// region Message Bus actions ------------------------------------------------------------------------------------------

// Publish messages to a channel (topic)
func (r *pubSubAdapter) Publish(messages ...IMessage) error {

	if len(messages) == 0 {
		return nil
	}

	// Create topics map
	topicsMap := make(map[string]*pubsub.Topic)
	for _, message := range messages {
		if _, ok := topicsMap[message.Topic()]; !ok {
			if topic, err := r.getOrCreateTopic(message.Topic()); err != nil {
				return err
			} else {
				topicsMap[message.Topic()] = topic
			}
		}
	}

	// Send messages to topic
	for _, message := range messages {
		if bytes, er := messageToRaw(message); er != nil {
			return er
		} else {
			if topic, ok := topicsMap[message.Topic()]; ok {
				_ = topic.Publish(context.Background(), &pubsub.Message{Data: bytes})
			}
		}
	}
	return nil
}

// Subscribe on topics
func (r *pubSubAdapter) Subscribe(factory MessageFactory, callback SubscriptionCallback, subscriberName string, topics ...string) (subscriptionId string, error error) {

	if len(topics) != 1 {
		return "", fmt.Errorf("only one topic allawed in this implementation")
	}

	topic, fe := r.getOrCreateTopic(topics[0])
	if fe != nil {
		return "", fe
	}

	sub, err := r.getOrCreateSubscription(topic, subscriberName)

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

	if er := sub.Receive(context.Background(), receiver); err != nil {
		return "", er
	} else {
		return sub.ID(), nil
	}
}

// Unsubscribe with the given subscriber id
func (r *pubSubAdapter) Unsubscribe(subscriptionId string) bool {
	err := r.client.Subscription(subscriptionId).Delete(context.Background())
	return err != nil
}

// Push Append one or multiple messages to a queue
func (r *pubSubAdapter) Push(messages ...IMessage) error {
	return fmt.Errorf("push is not supported in Google pubsub implementation, use Producer.Publish()")
}

// Pop Remove and get the last message in a queue or block until timeout expires
func (r *pubSubAdapter) Pop(factory MessageFactory, timeout time.Duration, queue ...string) (IMessage, error) {
	return nil, fmt.Errorf("pop is not supported in Google pubsub implementation, use Consumer.Subscribe()")
}

// CreateProducer creates message producer for specific topic
func (r *pubSubAdapter) CreateProducer(topicName string) (IMessageProducer, error) {
	if topic, err := r.getOrCreateTopic(topicName); err != nil {
		return nil, err
	} else {
		return &pubSubProducer{topicName: topicName, topic: topic}, nil
	}
}

// endregion

// region Producer actions ---------------------------------------------------------------------------------------------

type pubSubProducer struct {
	topicName string
	topic     *pubsub.Topic
}

// Close producer does noting in this implementation
func (p *pubSubProducer) Close() error {
	return nil
}

// Publish messages to a producer channel (topic)
func (p *pubSubProducer) Publish(messages ...IMessage) error {

	if len(messages) == 0 {
		return nil
	}

	if p.topic == nil {
		return fmt.Errorf("topic: %s not initialized", p.topicName)
	}

	// Send messages to topic (only messages wit the same topic name)
	for _, message := range messages {
		if bytes, er := messageToRaw(message); er != nil {
			return er
		} else {
			if message.Topic() == p.topicName {
				p.topic.Publish(context.Background(), &pubsub.Message{Data: bytes})
			}
		}
	}
	return nil
}

// endregion

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// Get topic or create it if not exists
func (r *pubSubAdapter) getOrCreateTopic(topicName string) (topic *pubsub.Topic, error error) {
	t := r.client.Topic(topicName)
	ok, err := t.Exists(context.Background())
	if err != nil {
		return nil, err
	}

	// If topic does not exist, create the topic
	if !ok {
		if t, err = r.client.CreateTopic(context.Background(), topicName); err != nil {
			return t, err
		}
	}
	return t, nil
}

// Get reference to existing subscription or create new topic if not exists
func (r *pubSubAdapter) getOrCreateSubscription(topic *pubsub.Topic, subscriberName string) (*pubsub.Subscription, error) {

	sub := r.client.Subscription(subscriberName)

	ok, err := sub.Exists(context.Background())
	if err != nil {
		return nil, err
	}

	// If Subscription does not exist, create the Subscription
	if !ok {
		sub, err = r.client.CreateSubscription(context.Background(), subscriberName, pubsub.SubscriptionConfig{Topic: topic})
		if err != nil {
			return nil, err
		} else {
			return sub, nil
		}
	}
	return sub, nil
}

// endregion
