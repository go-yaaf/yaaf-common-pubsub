package test

import (
	"fmt"
	"github.com/go-yaaf/yaaf-common/logger"
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

	// Set local pubsub emulator host
	os.Setenv("PUBSUB_EMULATOR_HOST", localEmulator)

	//project := "production,status:StatusAggregator,StatusLogger"
	//err := utils.DockerUtils().CreateContainer(containerImage).
	//	Name(containerName).
	//	Port("8681", "8681").
	//	Port("8682", "8682").
	//	Label("env", "test").
	//	Var("PUBSUB_PROJECT1", project).
	//	Run()
	//
	//assert.Nil(s.T(), err)

	// Give it 5 seconds to warm up
	time.Sleep(5 * time.Second)

	// Create and initialize
	s.mq = s.createSUT()
}

// TearDownSuite will be run once at the end of the testing suite, after all tests have been run
func (s *PubSubTestSuite) TearDownSuite() {
	//err := utils.DockerUtils().StopContainer(containerName)
	//assert.Nil(s.T(), err)
}

// createSUT creates the system-under-test which is postgresql implementation of IDatabase
func (s *PubSubTestSuite) createSUT() messaging.IMessageBus {

	uri := fmt.Sprintf("pubsub://%s", "pulseiot")
	sut, err := facilities.NewPubSubMessageBus(uri)
	if err != nil {
		panic(any(err))
	}

	if er := sut.Ping(5, 5); err != nil {
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

	// Create and run logger consumer
	NewStatusLogger(uri).Name("logger").Topic("status").Start()

	NewStatusLogger(uri).Name("viewer").Topic("status").Start()

	// Create and run average aggregator consumer
	// NewStatusAggregator(uri).Name("average").Topic("status").Duration(time.Minute).Interval(time.Second * 4).Start(wg)

	// Create status message publisher
	NewStatusPublisher(uri).Name("publisher").Topic("status").Duration(time.Minute).Interval(time.Second * 5).Start(wg)

	wg.Wait()
	logger.Info("Done")
}
