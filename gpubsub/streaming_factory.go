package gpubsub

import (
	"fmt"
	"net/url"

	. "github.com/go-yaaf/yaaf-common/messaging"
)

// StreamingFactory is a factory function that creates an appropriate IMessageBus based on the URI scheme.
// It parses the given URI and delegates the creation of the message bus to the corresponding factory.
//
// Supported schemes:
// - "pubsub": Creates a Google Cloud Pub/Sub message bus via NewPubSubMessageBus.
// - default: Creates an in-memory message bus for local development and testing via NewInMemoryMessageBus.
//
// The uri parameter is a string representing the connection URI for the messaging service.
// For Pub/Sub, the format is "pubsub://project-id".
//
// Returns an IMessageBus implementation and an error if the URI is invalid or the scheme is unsupported.
func StreamingFactory(uri string) (IMessageBus, error) {
	// Parse the URI to determine the messaging service scheme.
	parsedURI, err := url.Parse(uri)
	if err != nil {
		// Return an error if the URI is malformed.
		return nil, fmt.Errorf("error parsing STREAMING_URI (%s): %v", uri, err)
	}

	// Switch on the URI scheme to call the appropriate factory function.
	switch parsedURI.Scheme {
	case "pubsub":
		// For "pubsub" scheme, use the Google Cloud Pub/Sub message bus factory.
		return NewPubSubMessageBus(uri)
	default:
		// For any other scheme, default to an in-memory message bus.
		// This is useful for development, testing, or unsupported schemes.
		return NewInMemoryMessageBus()
	}
}
