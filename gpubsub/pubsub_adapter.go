package gpubsub

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/api/iterator"

	"github.com/go-yaaf/yaaf-common/config"
	. "github.com/go-yaaf/yaaf-common/entity"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

// Configuration keys (read via yaaf-common config, i.e. environment variables) and defaults
// used to harden the adapter. Defaults preserve the historical behaviour.
const (
	// cfgAutoCreateResources gates on-demand creation of topics/subscriptions.
	// When false, missing resources cause an error instead of being created implicitly.
	cfgAutoCreateResources = "PUBSUB_AUTO_CREATE_RESOURCES"
	// cfgMaxMessageBytes caps the size of an inbound message payload before it is unmarshalled.
	cfgMaxMessageBytes = "PUBSUB_MAX_MESSAGE_BYTES"

	// defaultMaxMessageBytes is Pub/Sub's own per-message limit (10 MiB).
	defaultMaxMessageBytes = 10 * 1024 * 1024
	// pingTimeout bounds a single connectivity probe attempt.
	pingTimeout = 10 * time.Second
)

// resourceNameRegex enforces Google Pub/Sub resource-id rules: 3-255 characters, starting with
// a letter, and containing only letters, digits and the characters . _ ~ % + -. Crucially it
// rejects '/', preventing an attacker-controlled name from reshaping the fully qualified resource
// path (e.g. "../other-project/topics/x").
var resourceNameRegex = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._~%+\-]{2,254}$`)

// validateResourceName verifies that a topic or subscription id is safe to interpolate into a
// fully qualified resource name. kind is used only for the error message ("topic"/"subscription").
func validateResourceName(kind, name string) error {
	if !resourceNameRegex.MatchString(name) {
		return fmt.Errorf("invalid %s name %q: must be 3-255 chars, start with a letter, and contain only letters, digits, and . _ ~ %% + -", kind, name)
	}
	if strings.HasPrefix(strings.ToLower(name), "goog") {
		return fmt.Errorf("invalid %s name %q: must not start with \"goog\"", kind, name)
	}
	return nil
}

// autoCreateResources reports whether missing topics/subscriptions may be created on demand.
func autoCreateResources() bool {
	return config.Get().GetBoolParamValueOrDefault(cfgAutoCreateResources, true)
}

// maxMessageBytes returns the configured inbound payload cap in bytes (<= 0 disables the check).
func maxMessageBytes() int {
	return config.Get().GetIntParamValueOrDefault(cfgMaxMessageBytes, defaultMaxMessageBytes)
}

// pubSubAdapter is a Google Pub/Sub implementation of the IMessageBus interface.
// It provides a way to interact with Google Cloud Pub/Sub for messaging operations.
type pubSubAdapter struct {
	client    *pubsub.Client // The Google Cloud Pub/Sub client.
	projectId string         // The Google Cloud project id (parsed from the URI).
	uri       string         // The connection URI for the Pub/Sub service.

	mu      sync.Mutex              // Guards the subscriptions map.
	cancels map[string]*subReceiver // Active Subscribe receive loops, keyed by subscription id.
}

// subReceiver holds the cancel func for a single Subscribe receive loop.
// It is stored by pointer so the owning goroutine can verify (by identity) that it
// is still the current registration before de-registering itself.
type subReceiver struct {
	cancel context.CancelFunc
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
		client:    client,
		projectId: projectId,
		uri:       URI,
		cancels:   make(map[string]*subReceiver),
	}

	return psa, nil
}

// topicPath builds the fully qualified topic name required by the v2 admin API.
// Format: projects/{projectId}/topics/{topicName}
func (r *pubSubAdapter) topicPath(topicName string) string {
	return fmt.Sprintf("projects/%s/topics/%s", r.projectId, topicName)
}

// subscriptionPath builds the fully qualified subscription name required by the v2 admin API.
// Format: projects/{projectId}/subscriptions/{subscriptionName}
func (r *pubSubAdapter) subscriptionPath(subscriptionName string) string {
	return fmt.Sprintf("projects/%s/subscriptions/%s", r.projectId, subscriptionName)
}

// Ping tests connectivity to the Pub/Sub service by issuing a lightweight ListTopics call
// (page size 1) against the configured project. It retries up to `retries` times, sleeping
// `intervalInSeconds` between attempts, and returns nil on the first success.
func (r *pubSubAdapter) Ping(retries uint, intervalInSeconds uint) error {
	if r.client == nil {
		return fmt.Errorf("pubsub client not initialized")
	}
	if retries == 0 {
		retries = 1
	}

	project := fmt.Sprintf("projects/%s", r.projectId)

	var lastErr error
	for attempt := uint(0); attempt < retries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(intervalInSeconds) * time.Second)
		}

		ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
		it := r.client.TopicAdminClient.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: project, PageSize: 1})
		_, err := it.Next()
		cancel()

		// An empty project (iterator.Done) still proves the round-trip succeeded.
		if err == nil || errors.Is(err, iterator.Done) {
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("pubsub ping failed after %d attempt(s): %v", retries, lastErr)
}

// Close stops all active subscription receive loops and closes the underlying Pub/Sub client.
// It's important to call this method to release resources.
func (r *pubSubAdapter) Close() error {
	// Cancel every active Subscribe receive loop so no goroutine outlives the client.
	r.mu.Lock()
	for id, s := range r.cancels {
		s.cancel()
		delete(r.cancels, id)
	}
	r.mu.Unlock()

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
	// Reject oversized payloads before spending memory/CPU unmarshalling them.
	if max := maxMessageBytes(); max > 0 && len(bytes) > max {
		return nil, fmt.Errorf("message size %d exceeds maximum allowed %d bytes", len(bytes), max)
	}
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
