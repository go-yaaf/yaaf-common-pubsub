# Pub/Sub Example

This example demonstrates how to use the `yaaf-common-pubsub` library to implement a simple pub/sub messaging system. It consists of a publisher that sends status messages, and two consumers: one that logs the messages and another that aggregates them.

## Overview

The example is composed of the following components:

- **`main.go`**: The entry point of the application. It initializes the message bus, creates the publisher and consumers, and starts them.
- **`model.go`**: Defines the `Status` data model and the `StatusMessage` that wraps it for messaging.
- **`status_publisher.go`**: A publisher that periodically sends `StatusMessage` messages to a "status" topic.
- **`status_logger.go`**: A consumer that subscribes to the "status" topic and logs the received messages.
- **`status_aggregator.go`**: A consumer that subscribes to the "status" topic, aggregates the status data over a time window, and logs the average values.

## How it Works

### Initialization

The `main` function in `main.go` starts by setting up the environment for the Google Cloud Pub/Sub emulator and creating a new `pubSubAdapter` instance. It then pings the message bus to ensure connectivity.

### Publisher

The `StatusPublisher` is responsible for generating and sending messages. It's configured with a topic, a duration to run, and an interval between messages. In its `run` method, it periodically creates a `StatusMessage` with random CPU and RAM values and publishes it to the "status" topic using `mq.Publish(message)`.

### Consumers

There are two consumers in this example:

1.  **`StatusLogger`**: This consumer subscribes to the "status" topic. For each message it receives, the `processMessage` function is called, which simply logs the content of the message. This demonstrates a simple message consumption pattern.

2.  **`StatusAggregator`**: This consumer also subscribes to the "status" topic. However, it performs a more complex task. It aggregates the `CPU` and `RAM` values from the received messages over a specific time interval. The `processMessage` function accumulates the values and the `logAggregatedStatus` function, which is called periodically, calculates the average and logs it. This showcases a stateful message processing pattern.

### Message Model

The `model.go` file defines the data structures used in the application:

-   **`Status`**: A simple struct that holds CPU and RAM usage data.
-   **`StatusMessage`**: A message struct that embeds the `Status` struct. This is the actual data that is sent over the message bus.

## Running the Example

To run this example, you need to have the Google Cloud Pub/Sub emulator running. You can start it with the following command:

```bash
gcloud beta emulators pubsub start --project=your-project-id
```
or you can run the `docker.compose.yml` file to run the Google Cloud Pub/Sub emulator and the PubSub UI in docker container

Then, you can run the example by executing:

```bash
go run .
```

You will see the logger printing the received messages and the aggregator printing the aggregated data.
