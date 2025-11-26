package main

import (
	ps "github.com/go-yaaf/yaaf-common-pubsub/gpubsub"
	"github.com/go-yaaf/yaaf-common/logger"
	"os"
	"sync"
	"time"
)

const (
	pubsubUri = "pubsub://your-project-id"
)

func init() {
	// Initialize logger
	logger.SetLevel("DEBUG")
	logger.Init()
}

func main() {

	// Set PUBSUB_EMULATOR_HOST environment variable to enable Google Cloud PubSub client to connect to the emulator
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", "0.0.0.0:8681")

	// Create instance of message bus
	bus, err := ps.NewPubSubMessageBus(pubsubUri)
	if err != nil {
		logger.Error("could not create message bus instance: %s", err.Error())
		return
	}
	// Try to connect to the Redis instance
	err = bus.Ping(3, 1)
	if err != nil {
		logger.Error("could not connect to the pubsub message bus, make sure the PubSub emulator instance is running: %s", err.Error())
		return
	}

	// Sync all publishers and consumers
	wg := &sync.WaitGroup{}
	wg.Add(2)

	// Create status message publisher
	NewStatusPublisher(bus).Name("publisher").Topic("status").Duration(time.Minute).Interval(time.Millisecond * 500).Start(wg)

	// Create and run logger consumer
	NewStatusLogger(bus).Name("status-logger").Topic("status").Start()

	// Create and run average aggregator consumer
	NewStatusAggregator(bus).Name("status-aggregator").Topic("status").Duration(time.Minute).Interval(time.Second * 5).Start(wg)

	wg.Wait()
	logger.Info("Done")

}
