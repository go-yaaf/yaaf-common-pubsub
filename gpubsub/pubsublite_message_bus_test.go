package gpubsub

import (
	"github.com/go-yaaf/yaaf-common/config"
	"github.com/go-yaaf/yaaf-common/logger"
	"os"
	"strconv"
	"testing"
	"time"

	//. "github.com/go-yaaf/yaaf-common/entity"
	. "github.com/go-yaaf/yaaf-common/messaging"
)

const testTopicName = "test_message_topic"

type TestRecord struct {
	BaseMessage
	Source         string   `json:"source"`         // Indication to the source location of the record
	AccountId      string   `json:"accountId"`      // Account ID
	StreamId       string   `json:"streamId"`       // Stream ID
	DeviceId       string   `json:"deviceId"`       // Device ID
	DeviceIp       string   `json:"deviceIp"`       // Device IP
	StartTime      int64    `json:"startTime"`      // Start window time in epoch time milliseconds
	EndTime        int64    `json:"endTime"`        // End window time in epoch time milliseconds
	SrcPortsList   []int    `json:"srcPortsList"`   // List of source ports
	SrcPortsCount  int      `json:"srcPortsCount"`  // Number of source ports
	DstPortsList   []int    `json:"dstPortsList"`   // List of destination ports
	DstPortsCount  int      `json:"dstPortsCount"`  // Number of destination ports
	EndpointsList  []string `json:"endpointsList"`  // List of outbound endpoints
	EndpointsCount int      `json:"endpointsCount"` // Number of outbound endpoints (IPs)
	SrcEndpoints   []string `json:"srcEndpoints"`   // List of outbound endpoints that sent data to the device
	DstEndpoints   []string `json:"dstEndpoints"`   // List of outbound endpoints that received data from the device
	PacketsIn      int      `json:"packetsIn"`      // Number of incoming packets (to the device)
	PacketsOut     int      `json:"packetsOut"`     // Number of outgoing packets (from the device)
	BytesIn        int      `json:"bytesIn"`        // Total number of incoming bytes (to the device)
	BytesOut       int      `json:"bytesOut"`       // Total number of outgoing bytes (from the device)
	SrcAckFlags    int      `json:"srcAckFlags"`    // Number of outgoing Ack flags (applicable only for TCP based protocols)
	DstAckFlags    int      `json:"dstAckFlags"`    // Number of incoming Ack flags (applicable only for TCP based protocols)
	SrcSynFlags    int      `json:"srcSynFlags"`    // Number of outgoing Syn flags (applicable only for TCP based protocols)
	DstSynFlags    int      `json:"dstSynFlags"`    // Number of incoming Syn flags (applicable only for TCP based protocols)
	SrcRstFlags    int      `json:"srcRstFlags"`    // Number of outgoing Rst flags (applicable only for TCP based protocols)
	DstRstFlags    int      `json:"dstRstFlags"`    // Number of incoming Rst flags (applicable only for TCP based protocols)
	Labels         []string `json:"labels"`         // List of labels
}

func (tm *TestRecord) TABLE() string { return "test_message" }
func (tm *TestRecord) KEY() string   { return tm.MsgSessionId }

type TestRecordMessage Message[TestRecord]

func NewTestRecordMessage() IMessage {
	return &TestRecordMessage{}
}

func initNewTestRecordMessage(id int) IMessage {
	payload := TestRecord{
		AccountId: "OnWave",
		StreamId:  "onWave-1",
		DeviceId:  "device-1",
	}

	msg := &TestRecordMessage{
		BaseMessage: BaseMessage{
			MsgTopic:     testTopicName,
			MsgOpCode:    1,
			MsgVersion:   "1",
			MsgAddressee: "",
			MsgSessionId: strconv.Itoa(id),
		},
		MsgPayload: payload,
	}
	return msg
}

func init() {
	logger.SetTimeLayout("02-01 15:04:05.000")
	logger.SetLevel("debug")
	logger.EnableJsonFormat(false)
	logger.Init()
}

func TestPubSubLitePublish(t *testing.T) {

	_ = os.Setenv("STREAMING_URI", "pubsublite://projects/shieldiot-staging/locations/europe-west3-a")

	uri := config.Get().StreamingUri()

	bus, err := StreamingFactory(uri)

	if err != nil {
		t.Fatalf("error create message bus: %s", err)
	}

	i := 0
	callbackWrapper := func(id int, delay time.Duration) SubscriptionCallback {
		return func(m IMessage) bool {
			i++
			logger.Debug("subscriber: %d   %d message received, id: %s", id, i, m.SessionId())
			if delay > 0 {
				time.Sleep(time.Millisecond * delay)
			}
			return true
		}
	}

	producer, err := bus.CreateProducer(testTopicName)
	if err != nil {
		t.Fatalf("error create producer: %s", err)
	}

	_ = producer.Publish(nil)

	if _, err := bus.Subscribe("test_subscriber", NewTestRecordMessage, callbackWrapper(1, 0), testTopicName); err != nil {
		t.Fatalf("error subscrubing to test topic: ")
	}

	if _, err := bus.Subscribe("test_subscriber", NewTestRecordMessage, callbackWrapper(2, 200), testTopicName); err != nil {
		t.Fatalf("error subscrubing to test topic: ")
	}
	go func() {
		i := 0
		for {
			i++
			msg := initNewTestRecordMessage(i)
			if err = producer.Publish(msg); err != nil {
				t.Fatalf("error publish message: %s", err)
			}
		}

	}()

	select {}
}
