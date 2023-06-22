package test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"os"
	"testing"
)

func TestPubSubEmulator(t *testing.T) {
	skipCI(t)

	// Config emulator
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8681")

	// Connect
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, "shieldiot-production")
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	// Add topic
	createTopic(client, "status")

	if ok, er := client.Topic("newTopic").Exists(ctx); er != nil {
		t.Error(er.Error())
	} else {
		fmt.Println("Topic exists?:", ok)
	}

	if topic, er := client.CreateTopic(ctx, "newTopic"); er != nil {
		fmt.Println("Topic exists")
	} else {
		fmt.Println("Topic string:", topic.String(), "Id:", topic.ID())
	}

	// List topics
	it := client.Topics(ctx)
	for topic, er := it.Next(); er == nil; topic, er = it.Next() {
		fmt.Println(topic.ID())
		_ = topic.Publish(ctx, &pubsub.Message{Data: ([]byte)("Hello world")})
	}

}

func createTopic(client *pubsub.Client, topicName string) {
	if topic, er := client.CreateTopic(context.Background(), topicName); er != nil {
		fmt.Println("Topic exists")
	} else {
		fmt.Println("Topic string:", topic.String(), "Id:", topic.ID())
	}
}

func createSubscriber(client *pubsub.Client, topicName string, subscriberName string) {

	sub := client.Subscription(subscriberName)
	ok, err := sub.Exists(context.Background())
	if err != nil {
		fmt.Println("Subscription exists error", err.Error())
		return
	}

	// If Subscription does not exist, create the Subscription
	if !ok {

		sub, err = client.CreateSubscription(context.Background(), subscriberName, pubsub.SubscriptionConfig{Topic: client.Topic(topicName)})
		if err != nil {
			fmt.Println("createSubscriber error", err.Error())
			return
		}
	}
}
