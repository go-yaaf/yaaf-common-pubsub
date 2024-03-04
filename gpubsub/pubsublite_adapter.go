package gpubsub

import (
	"cloud.google.com/go/pubsublite"
	"context"
	"errors"
	"fmt"
	. "github.com/go-yaaf/yaaf-common/messaging"
	"strings"
)

type pubSubLiteAdapter struct {
	client *pubsublite.AdminClient
	uri,
	//gcpRegion,
	gcpLocation,
	gcpProject string
}

// NewPubSubLiteMessageBus factory method for PubSub IMessageBus implementation
//
// param: URI - represents the redis connection string in the format of: pubsub://project/region
func NewPubSubLiteMessageBus(uri string) (mq IMessageBus, err error) {

	var (
		client            *pubsublite.AdminClient
		project, location string
	)

	if project, err = extractGcpProjectNameFromUri(uri); err != nil {
		return nil, err
	}

	if location, err = extractGcpLocation(uri); err != nil {
		return nil, err
	}

	ctx := context.Background()

	if client, err = pubsublite.NewAdminClient(ctx, gcpZoneToRegion(location)); err != nil {
		return nil, err
	}

	psa := &pubSubLiteAdapter{
		uri:         uri,
		client:      client,
		gcpProject:  project,
		gcpLocation: location,
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
	return NewPubSubMessageBus("")
}

// extractGcpProjectNameFromUri extracts the project name from a Pub/Sub Lite URI.
func extractGcpProjectNameFromUri(uri string) (string, error) {
	parts := strings.Split(uri, "/")
	if len(parts) != 4 || parts[0] != "pubsub:" {
		return "", errors.New("invalid URI format")
	}
	return parts[2], nil
}

// extractGcpLocation extracts the location (region or zone) from a Pub/Sub Lite URI.
func extractGcpLocation(uri string) (string, error) {
	parts := strings.Split(uri, "/")
	if len(parts) != 4 || parts[0] != "pubsub:" {
		return "", errors.New("invalid URI format")
	}
	return parts[3], nil
}

// GcpZoneToRegion converts a zone name to its corresponding region name.
// Zones are typically in the format of [region]-[zone_identifier] (e.g., europe-west3-a).
// The function removes the zone identifier to return the region name.
func gcpZoneToRegion(zone string) string {
	if idx := strings.LastIndex(zone, "-"); idx != -1 {
		return zone[:idx]
	}
	return zone // Return the original string if no zone identifier is found.
}
