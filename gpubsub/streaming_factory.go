package gpubsub

import (
	"fmt"
	"net/url"

	. "github.com/go-yaaf/yaaf-common/messaging"
)

// StreamingFactory - Analyzes the URI string and calls the appropriate factory function
func StreamingFactory(uri string) (IMessageBus, error) {
	// Parse the URI
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("error parsing STREAMING_URI (%s): %s", uri, err) // Return error if the URI is not valid
	}

	// Depending on the scheme, call the appropriate factory function
	switch parsedURI.Scheme {
	case "pubsub":
		return NewPubSubMessageBus(uri)
	default:
		return NewInMemoryMessageBus()
	}
}
