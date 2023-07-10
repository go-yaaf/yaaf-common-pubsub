package test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/stretchr/testify/require"
	"testing"
)

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

func TestIsJsonString(t *testing.T) {
	skipCI(t)

	status := NewStatus1(100, 48)
	msg := newStatusMessage("topic", status.(*Status), "session")

	bytes, err := entity.Marshal(msg)
	require.NoError(t, err)
	isJson := isJsonString(bytes)
	fmt.Println(isJson)

	bytes = []byte{04, 45, 23, 56, 44, 45, 23, 56, 44, 45, 23, 56, 44}
	isJson = isJsonString(bytes)
	fmt.Println(isJson)
}

// Check if the byte array representing a JSON string
func isJsonString(bytes []byte) bool {

	s := string(bytes[0:1])
	e := string(bytes[len(bytes)-1:])
	fmt.Println(s, e, len(bytes))

	if string(bytes[0:1]) == "{" && string(bytes[len(bytes)-1:]) == "}" {
		return true
	}
	if string(bytes[0:1]) == "[" && string(bytes[len(bytes)-1:]) == "]" {
		return true
	}
	return false
}
