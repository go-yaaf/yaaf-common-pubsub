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
				if _, err := topic.Publish(context.Background(), &pubsub.Message{Data: bytes}).Get(context.Background()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Subscribe on topics
func (r *pubSubAdapter) Subscribe(subscriberName string, factory MessageFactory, callback SubscriptionCallback, topics ...string) (subscriptionId string, error error) {

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

	go func() {
		ctx := context.Background()
		sId := sub.ID()
		if er := sub.Receive(ctx, receiver); er != nil {
			logger.Error("Subscription %s receive error: %s", sId, er.Error())
		}
		if er := sub.Delete(ctx); er != nil {
			logger.Error("Subscription %s delete error: %s", sId, err.Error())
		}
		logger.Error("Subscription %s ends", sId)
	}()
	return sub.ID(), nil
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

// CreateConsumer creates message consumer for a specific topic
func (r *pubSubAdapter) CreateConsumer(subscriberName string, mf MessageFactory, topics ...string) (IMessageConsumer, error) {
	if len(topics) != 1 {
		return nil, fmt.Errorf("only one topic allawed in this implementation")
	}

	topic, fe := r.getOrCreateTopic(topics[0])
	if fe != nil {
		return nil, fe
	}

	sub, err := r.getOrCreateSubscription(topic, subscriberName)
	if err != nil {
		return nil, err
	}

	return &pubSubConsumer{topicName: topics[0], subscription: sub, factory: mf}, nil
}

// endregion

// region Producer actions ---------------------------------------------------------------------------------------------

type pubSubProducer struct {
	topicName string
	topic     *pubsub.Topic
}

// Close producer does nothing in this implementation
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
				msg := &pubsub.Message{Data: bytes}
				if p.topic.EnableMessageOrdering && message.Addressee() != "" {
					msg.OrderingKey = message.Addressee()
				}
				if _, err := p.topic.Publish(context.Background(), msg).Get(context.Background()); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// endregion

// region Consumer actions ---------------------------------------------------------------------------------------------

type pubSubConsumer struct {
	topicName    string
	subscription *pubsub.Subscription
	factory      MessageFactory
}

// Close producer does nothing in this implementation
func (c *pubSubConsumer) Close() error {
	return nil
}

// Read message from topic, blocks until a new message arrive or until timeout expires
// Use 0 instead of time.Duration for unlimited time
// The standard way to use Read is by using infinite loop:
//
//	for {
//		if msg, err := consumer.Read(time.Second * 5); err != nil {
//			// Handle error
//		} else {
//			// Process message in a dedicated go routine
//			go processTisMessage(msg)
//		}
//	}
func (c *pubSubConsumer) Read(timeout time.Duration) (message IMessage, err error) {

	ctx, cancel := context.WithCancel(context.Background())
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}

	er := c.subscription.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
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

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// Get topic or create it if not exists
func (r *pubSubAdapter) getOrCreateTopic(topicName string) (topic *pubsub.Topic, err error) {

	var exists bool

	topic = r.client.Topic(topicName)

	// Check if the topic exists.
	exists, err = topic.Exists(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to check if topic %s exists, error: %s", topicName, err)
	}
	// Create the topic if it does not exist.
	if !exists {
		topic, err = r.client.CreateTopic(context.Background(), topicName)
		if err != nil {
			return nil, fmt.Errorf("failed to create topic: %s, error: %s", topicName, err)
		}
	}

	// Configure message ordering if enabled.
	if config.Get().EnableMessageOrdering() {
		topic.EnableMessageOrdering = true
	}
	return
}

// Get reference to existing subscription or create new topic if not exists
func (r *pubSubAdapter) getOrCreateSubscription(topic *pubsub.Topic, subscriberName string) (sub *pubsub.Subscription, err error) {

	var exists bool

	cfg := config.Get()

	defer func() {
		if sub != nil {
			sub.ReceiveSettings.NumGoroutines = cfg.PubSubNumOfGoroutines()
			sub.ReceiveSettings.MaxOutstandingMessages = cfg.PubSubMaxOutstandingMessages()
			sub.ReceiveSettings.MaxOutstandingBytes = cfg.PubSubMaxOutstandingBytes()
		}
	}()

	sub = r.client.Subscription(subscriberName)

	exists, err = sub.Exists(context.Background())
	if err != nil {
		sub = nil
		return
	}

	// If Subscription does not exist, create the Subscription
	if !exists {
		sub, err = r.client.CreateSubscription(context.Background(), subscriberName,
			pubsub.SubscriptionConfig{Topic: topic,
				AckDeadline:           time.Second * time.Duration(cfg.PubSubAckDeadline()),
				EnableMessageOrdering: cfg.EnableMessageOrdering()})
		if err != nil {
			sub = nil
		}
	}
	return
}

// endregion
