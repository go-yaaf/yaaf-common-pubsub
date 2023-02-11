package gpubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"cloud.google.com/go/pubsub"

	. "github.com/go-yaaf/yaaf-common/messaging"
)

type pubSubAdapter struct {
	client *pubsub.Client
}

// NewPubSubMessageBus factory method for PubSub IMessageBus implementation
//
// param: URI - represents the redis connection string in the format of: pubsub://projectId
func NewPubSubMessageBus(URI string) (mq IMessageBus, err error) {

	uri, err := url.Parse(URI)

	if err != nil {
		return nil, err
	}

	projectId := uri.Host

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return nil, err
	}
	psa := &pubSubAdapter{
		client: client,
	}

	return psa, nil
}

// Ping Test connectivity for retries number of time with time interval (in seconds) between retries
func (r *pubSubAdapter) Ping(retries uint, intervalInSeconds uint) error {

	if r.client == nil {
		return fmt.Errorf("pubsub client not initialized")
	}

	// TODO: implement PING method
	return nil
}

func (r *pubSubAdapter) Close() error {
	return r.client.Close()
}

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// convert raw data to message
func rawToMessage(factory MessageFactory, bytes []byte) (IMessage, error) {
	message := factory()
	if err := json.Unmarshal(bytes, &message); err != nil {
		return nil, err
	} else {
		return message, nil
	}
}

// convert message to raw data
func messageToRaw(message IMessage) ([]byte, error) {
	return json.Marshal(message)
}

// endregion
