package test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"

	facilities "github.com/go-yaaf/yaaf-common-pubsub/gpubsub"
)

func TestPubSubBinary(t *testing.T) {
	skipCI(t)

	_ = os.Setenv("PUBSUB_EMULATOR_HOST", localEmulator)

	// Give it 5 seconds to warm up
	time.Sleep(5 * time.Second)

	uri := fmt.Sprintf("pubsub://%s", "production")
	bus, err := facilities.NewPubSubMessageBus(uri)
	if err != nil {
		panic(any(err))
	}

	if er := bus.Ping(5, 5); er != nil {
		fmt.Println("error pinging database")
		panic(any(er))
	}

	status := NewStatus1(100, 48)
	message := newStatusMessage("topic", status.(*Status), "session")

	err = bus.Publish(message)
	require.NoError(t, err)

	fmt.Println("Done")
}
