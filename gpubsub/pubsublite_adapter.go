package gpubsub

import (
	"cloud.google.com/go/pubsublite"
	"context"
	"fmt"
	. "github.com/go-yaaf/yaaf-common/messaging"
	"net/url"
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
	parts, err := parseAndValidatePubSubLiteUri(uri)
	if err != nil {
		return "", fmt.Errorf("error extracting GCP project name from URI (%s): %s", uri, err)
	}
	return parts[1], nil
}

// extractGcpLocation extracts the location (region or zone) from a Pub/Sub Lite URI.
func extractGcpLocation(uri string) (string, error) {
	parts, err := parseAndValidatePubSubLiteUri(uri)
	if err != nil {
		return "", fmt.Errorf("error extracting GCP location from URI (%s): %s", uri, err)
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

// parseAndValidatePubSubLiteUri parses the URI and validates its scheme and format.
func parseAndValidatePubSubLiteUri(uri string) ([]string, error) {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("error parsing URI (%s): %s", uri, err)
	}
	if parsedURI.Scheme != "pubsublite" {
		return nil, fmt.Errorf("invalid scheme (%s) in URI (%s)", parsedURI.Scheme, uri)
	}

	parts := strings.Split(parsedURI.Path, "/")
	if len(parts) != 4 {
		return nil, fmt.Errorf("invalid URI format: %s", uri)
	}

	return parts, nil
}
