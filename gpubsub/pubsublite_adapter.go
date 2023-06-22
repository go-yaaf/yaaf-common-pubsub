package gpubsub

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/pubsublite"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

type pubSubLiteAdapter struct {
	client     *pubsublite.AdminClient
	uri        string
	gcpProject string
	gcpRegion  string
	gcpZone    string
}

// NewPubSubLiteMessageBus factory method for PubSub IMessageBus implementation
//
// param: URI - represents the redis connection string in the format of: pubsub://project/region
func NewPubSubLiteMessageBus(URI string) (mq IMessageBus, err error) {

	project := getProject(URI)
	region := getRegion(URI)
	zone := getRegion(URI)

	ctx := context.Background()

	client, err := pubsublite.NewAdminClient(ctx, region)
	if err != nil {
		return nil, err
	}

	psa := &pubSubLiteAdapter{
		uri:        URI,
		client:     client,
		gcpProject: project,
		gcpRegion:  region,
		gcpZone:    zone,
	}

	return psa, nil
}

// Ping Test connectivity for retries number of time with time interval (in seconds) between retries
func (r *pubSubLiteAdapter) Ping(retries uint, intervalInSeconds uint) error {

	if r.client == nil {
		return fmt.Errorf("pubsub client not initialized")
	}

	// TODO: implement PING method
	return nil
}

func (r *pubSubLiteAdapter) Close() error {
	return r.client.Close()
}

// CloneMessageBus returns a new copy of this message bus
func (r *pubSubLiteAdapter) CloneMessageBus() (mq IMessageBus, err error) {
	return NewPubSubMessageBus(r.uri)
}

// Get region or zone
func getRegion(URI string) string {

	// First, try to extract region (zone) from the URI
	if uri, err := url.Parse(URI); err == nil {
		zone := uri.Path
		return strings.Replace(zone, "/", "", -1)
	}

	if result := os.Getenv("GCP_REGION"); len(result) > 0 {
		return result
	} else {
		return "europe-west3-a"
	}
}

// Get project
func getProject(URI string) string {

	// First, try to extract project from the URI
	if uri, err := url.Parse(URI); err == nil {
		return uri.Host
	}

	// Try to get the project from the environment
	if result := os.Getenv("GCP_PROJECT"); len(result) > 0 {
		return result
	} else {
		return "shieldiot-production"
	}
}
