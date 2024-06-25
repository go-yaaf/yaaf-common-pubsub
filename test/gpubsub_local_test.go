package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"sync"
	"testing"
	"time"

	facilities "github.com/go-yaaf/yaaf-common-pubsub/gpubsub"

	"github.com/go-yaaf/yaaf-common/messaging"
	"github.com/stretchr/testify/suite"
)

const (
	containerPort  = "8681"
	containerName  = "pub_sub_emulator"
	containerImage = "messagebird/gcloud-pubsub-emulator"
	localEmulator  = "localhost:8681"
)

// RedisQueueTestSuite creates a Redis container with data for a suite of message queue tests and release it when done
type PubSubTestSuite struct {
	suite.Suite
	containerID string
	mq          messaging.IMessageBus
}

func TestPubSubSuite(t *testing.T) {
	skipCI(t)
	suite.Run(t, new(PubSubTestSuite))
}

// SetupSuite will run once when the test suite begins
func (s *PubSubTestSuite) SetupSuite() {

	// Set local pub-sub emulator host
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", localEmulator)

	// Give it 5 seconds to warm up
	time.Sleep(5 * time.Second)

	// Create and initialize
	s.mq = s.createSUT()
}

// TearDownSuite will be run once at the end of the testing suite, after all tests have been run
func (s *PubSubTestSuite) TearDownSuite() {
	err := utils.DockerUtils().StopContainer(containerName)
	assert.Nil(s.T(), err)
}

// createSUT creates the system-under-test which is postgresql implementation of IDatabase
func (s *PubSubTestSuite) createSUT() messaging.IMessageBus {

	uri := fmt.Sprintf("pubsub://%s", "pulseiot")
	sut, err := facilities.NewPubSubMessageBus(uri)
	if err != nil {
		panic(any(err))
	}

	if er := sut.Ping(5, 5); er != nil {
		fmt.Println("error pinging database")
		panic(any(er))
	}

	return sut
}

func (s *PubSubTestSuite) TestLocalPubSub() {

	uri := fmt.Sprintf("pubsub://%s", "pulseiot")

	// Sync all publishers and consumers
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Create subscriber
	if subscriber, err := s.mq.Subscribe("my_subscriber", NewStatusMessage, s.processMessage, "status"); err != nil {
		logger.Error(err.Error())
	} else {
		logger.Info("subscriber: %s was created", subscriber)
	}

	// Create status message publisher
	NewStatusPublisher(uri).Name("publisher").Topic("status").Duration(time.Minute).Interval(time.Second * 10).Start(wg)

	wg.Wait()
	logger.Info("Done")

}

func (s *PubSubTestSuite) processMessage(message messaging.IMessage) bool {
	logger.Info("[my_subscriber] message: %s", message.SessionId())
	return true
}

// Use consumer -> reader pattern to read messages
func (s *PubSubTestSuite) reader() {

	consumer, err := s.mq.CreateConsumer("status_reader", NewStatusMessage, "status")
	if err != nil {
		logger.Error(err.Error())
	} else {
		logger.Info("Starting consumer: status_reader")
	}

	for {
		if msg, err := consumer.Read(time.Second * 5); err != nil {
			logger.Error(err.Error())
		} else {
			logger.Info("[reader] --> message: %s received", msg.SessionId())
		}
	}
}
