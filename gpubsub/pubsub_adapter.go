package gpubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"net/url"

	. "github.com/go-yaaf/yaaf-common/entity"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

// pubSubAdapter is a Google Pub/Sub implementation of the IMessageBus interface.
// It provides a way to interact with Google Cloud Pub/Sub for messaging operations.
type pubSubAdapter struct {
	client *pubsub.Client // The Google Cloud Pub/Sub client.
	uri    string         // The connection URI for the Pub/Sub service.
}

// NewPubSubMessageBus is a factory method that creates a new Pub/Sub IMessageBus implementation.
// It initializes a new Pub/Sub client and returns a pubSubAdapter instance.
// The URI parameter should be in the format: pubsub://projectId
func NewPubSubMessageBus(URI string) (mq IMessageBus, err error) {

	uri, err := url.Parse(URI)
	if err != nil {
		return nil, fmt.Errorf("invalid URI: %s, error: %v", URI, err)
	}

	projectId := uri.Host
	if projectId == "" {
		return nil, fmt.Errorf("projectId is missing in URI: %s", URI)
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %v", err)
	}
	psa := &pubSubAdapter{
		client: client,
		uri:    URI,
	}

	return psa, nil
}

// Ping tests the connectivity to the Pub/Sub service.
// It takes a number of retries and the interval in seconds between retries.
// Note: This method is not yet implemented.
func (r *pubSubAdapter) Ping(retries uint, intervalInSeconds uint) error {
	if r.client == nil {
		return fmt.Errorf("pubsub client not initialized")
	}
	// TODO: Implement a proper Ping method for Google Cloud Pub/Sub.
	// This could involve listing topics or a similar operation to check connectivity.
	return nil
}

// Close closes the underlying Pub/Sub client.
// It's important to call this method to release resources.
func (r *pubSubAdapter) Close() error {
	if r.client != nil {
		return r.client.Close()
	}
	return nil
}

// CloneMessageBus creates and returns a new instance of the pubSubAdapter.
// This allows for creating separate connections or channels for different parts of an application.
func (r *pubSubAdapter) CloneMessageBus() (mq IMessageBus, err error) {
	return NewPubSubMessageBus(r.uri)
}

// region PRIVATE SECTION ----------------------------------------------------------------------------------------------

// rawToMessage converts a byte slice into an IMessage.
// It uses the provided MessageFactory to create an instance of the message
// and then unmarshals the byte slice into it.
func rawToMessage(factory MessageFactory, bytes []byte) (IMessage, error) {
	message := factory()
	if err := Unmarshal(bytes, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %v", err)
	}
	return message, nil
}

// messageToRaw converts an IMessage into a byte slice.
// It marshals the message into a JSON byte representation.
func messageToRaw(message IMessage) ([]byte, error) {
	bytes, err := Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %v", err)
	}
	return bytes, nil
}

// isJsonString checks if a byte slice is a JSON string.
// It performs a basic check for '{' and '}' or '[' and ']' at the start and end of the slice.
func isJsonString(bytes []byte) bool {
	if len(bytes) < 2 {
		return false
	}
	// Check for object or array JSON structure
	if (bytes[0] == '{' && bytes[len(bytes)-1] == '}') || (bytes[0] == '[' && bytes[len(bytes)-1] == ']') {
		return true
	}
	return false
}

// endregion
