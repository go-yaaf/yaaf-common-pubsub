package test

import (
	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"context"
	"fmt"
	"github.com/go-yaaf/yaaf-common/entity"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

func createTopic(client *pubsub.Client, projectId, topicName string) {
	name := fmt.Sprintf("projects/%s/topics/%s", projectId, topicName)
	if topic, er := client.TopicAdminClient.CreateTopic(context.Background(), &pubsubpb.Topic{Name: name}); er != nil {
		fmt.Println("Topic exists")
	} else {
		fmt.Println("Topic name:", topic.GetName())
	}
}

func createSubscriber(client *pubsub.Client, projectId, topicName string, subscriberName string) {

	subName := fmt.Sprintf("projects/%s/subscriptions/%s", projectId, subscriberName)
	topicPath := fmt.Sprintf("projects/%s/topics/%s", projectId, topicName)

	_, err := client.SubscriptionAdminClient.GetSubscription(context.Background(), &pubsubpb.GetSubscriptionRequest{Subscription: subName})
	if err == nil {
		return
	}
	if status.Code(err) != codes.NotFound {
		fmt.Println("Subscription exists error", err.Error())
		return
	}

	// If the subscription does not exist, create it
	_, err = client.SubscriptionAdminClient.CreateSubscription(context.Background(), &pubsubpb.Subscription{
		Name:  subName,
		Topic: topicPath,
	})
	if err != nil {
		fmt.Println("createSubscriber error", err.Error())
		return
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
