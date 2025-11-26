# GO-YAAF Google Pub/Sub Messaging Middleware
![Project status](https://img.shields.io/badge/version-1.2-green.svg)
[![Build](https://github.com/go-yaaf/yaaf-common-pubsub/actions/workflows/build.yml/badge.svg)](https://github.com/go-yaaf/yaaf-common-pubsub/actions/workflows/build.yml)
[![Coverage Status](https://coveralls.io/repos/go-yaaf/yaaf-common-pubsub/badge.svg?branch=main&service=github)](https://coveralls.io/github/go-yaaf/yaaf-common-pubsub?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-yaaf/yaaf-common-pubsub)](https://goreportcard.com/report/github.com/go-yaaf/yaaf-common-pubsub)
[![GoDoc](https://godoc.org/github.com/go-yaaf/yaaf-common-pubsub?status.svg)](https://pkg.go.dev/github.com/go-yaaf/yaaf-common-pubsub)
![License](https://img.shields.io/dub/l/vibe-d.svg)

This library provides a [Google Cloud Pub/Sub](https://cloud.google.com/pubsub) implementation of the `IMessageBus` interface from the `yaaf-common` library. It enables seamless integration of Google's scalable and reliable messaging service into any application built with the YAAF framework.

## What It Does

This library acts as a middleware, abstracting the complexities of interacting with Google Cloud Pub/Sub. It provides a clean, interface-driven way to perform common messaging operations:

- **Publish** messages to topics.
- **Subscribe** to topics and process messages asynchronously.
- Create dedicated **Producers** for high-throughput message publishing.
- Create **Consumers** for controlled message consumption.
- Automatic topic and subscription creation.
- Support for the local Google Cloud Pub/Sub emulator for development and testing.

## Installation

To add the library to your project, use `go get`:
```bash
go get -v -t github.com/go-yaaf/yaaf-common-pubsub
```

Then, import the package into your code:
```go
import "github.com/go-yaaf/yaaf-common-pubsub/gpubsub"
```

## How to Use It

### 1. Creating a Message Bus

First, create an instance of the Pub/Sub message bus. The factory function `NewPubSubMessageBus` requires a connection URI in the format `pubsub://<your-gcp-project-id>`.

To connect to a local emulator, you must also set the `PUBSUB_EMULATOR_HOST` environment variable.

```go
package main

import (
	"os"
	"github.com/go-yaaf/yaaf-common/logger"
	ps "github.com/go-yaaf/yaaf-common-pubsub/gpubsub"
)

func main() {
	// Use the Pub/Sub emulator for local development
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8681")

	// Create a new message bus instance
	bus, err := ps.NewPubSubMessageBus("pubsub://your-project-id")
	if err != nil {
		logger.Fatal("could not create message bus: %v", err)
	}
	defer bus.Close()

	// Ping to verify connectivity
	if err := bus.Ping(3, 1); err != nil {
		logger.Fatal("could not connect to pub/sub: %v", err)
	}
	logger.Info("Message bus connected successfully!")
}
```

### 2. Defining a Message

Messages must implement the `IMessage` interface from `yaaf-common`. Here is an example of a custom message type.

```go
import (
    "fmt"
    . "github.com/go-yaaf/yaaf-common/entity"
    . "github.com/go-yaaf/yaaf-common/messaging"
)

// Status represents our data model
type Status struct {
    BaseEntity
    CPU int `json:"cpu"`
    RAM int `json:"ram"`
}

// StatusMessage is the message wrapper for Status
type StatusMessage struct {
    BaseMessage
    Status *Status `json:"status"`
}

func (m *StatusMessage) Payload() any { return m.Status }

// NewStatusMessage is a factory function for creating an empty message, required for unmarshaling
func NewStatusMessage() IMessage {
    return &StatusMessage{}
}

// newStatusMessage is a helper to create a new message with data
func newStatusMessage(topic string, cpu, ram int) IMessage {
    status := &Status{
        BaseEntity: BaseEntity{Id: NanoID(), CreatedOn: Now()},
        CPU:        cpu,
        RAM:        ram,
    }
    message := &StatusMessage{Status: status}
    message.MsgTopic = topic
    return message
}
```

### 3. Publishing Messages

You can publish one or more messages directly from the bus instance. The library will automatically create the topic if it doesn't exist.

```go
// Create a message
msg := newStatusMessage("status-topic", 80, 65)

// Publish the message
if err := bus.Publish(msg); err != nil {
    logger.Error("failed to publish message: %v", err)
} else {
    logger.Info("Message published successfully!")
}
```

### 4. Subscribing to a Topic

To receive messages, subscribe to a topic with a callback function. The library handles message acknowledgment based on the boolean return value of your callback (`true` for ACK, `false` for NACK).

```go
// Define a callback function to process messages
func handleMessage(message IMessage) bool {
    if sm, ok := message.(*StatusMessage); ok {
        logger.Info("Received status: CPU=%d%%, RAM=%d%%", sm.Status.CPU, sm.Status.RAM)
        return true // Acknowledge the message
    }
    logger.Warn("Received unknown message type")
    return false // Nacknowledge the message (will be redelivered)
}

// Subscribe to the topic
subscriptionId, err := bus.Subscribe("my-subscriber-id", NewStatusMessage, handleMessage, "status-topic")
if err != nil {
    logger.Error("failed to subscribe: %v", err)
} else {
    logger.Info("Subscribed with ID: %s", subscriptionId)
}

// Block to keep the application running
select {}
```

### 5. Using a Producer and Consumer

For more structured code, you can create dedicated producers and consumers.

#### Producer

A producer is bound to a specific topic and is useful for services that only need to publish messages.

```go
producer, err := bus.CreateProducer("status-topic")
if err != nil {
    logger.Fatal("failed to create producer: %v", err)
}
defer producer.Close()

msg := newStatusMessage("status-topic", 75, 55)
if err := producer.Publish(msg); err != nil {
    logger.Error("producer failed to publish: %v", err)
}
```

#### Consumer

A consumer allows you to read messages one by one in a blocking manner, giving you more control over message processing flow.

```go
consumer, err := bus.CreateConsumer("my-consumer-id", NewStatusMessage, "status-topic")
if err != nil {
    logger.Fatal("failed to create consumer: %v", err)
}
defer consumer.Close()

for {
    // Read a message with a 10-second timeout
    msg, err := consumer.Read(10 * time.Second)
    if err != nil {
        logger.Warn("failed to read message (timeout?): %v", err)
        continue
    }

    if sm, ok := msg.(*StatusMessage); ok {
        logger.Info("Consumer read status: CPU=%d%%, RAM=%d%%", sm.Status.CPU, sm.Status.RAM)
    }
}
```

## Testing with the Emulator

To test this library locally without connecting to a live GCP environment, you can use the Google Cloud Pub/Sub emulator.

1.  **Install the emulator**:
    ```bash
    gcloud components install pubsub-emulator
    gcloud components update
    ```

2.  **Start the emulator**:
    ```bash
    gcloud beta emulators pubsub start --project=your-project-id
    ```

3.  **Set the environment variable** in your application:
    The emulator will print the host and port it's running on (e.g., `localhost:8681`). Set this address as an environment variable before running your application.
    ```bash
    export PUBSUB_EMULATOR_HOST="localhost:8681"
    ```

## Full Example

For a complete, runnable example demonstrating a publisher and multiple consumers, see the [Pub/Sub Example](./examples/pubsub/README.md).
