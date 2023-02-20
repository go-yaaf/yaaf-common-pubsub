package test

import (
	"fmt"
	. "github.com/go-yaaf/yaaf-common/entity"
	. "github.com/go-yaaf/yaaf-common/messaging"
	"time"
)

// region Status data Model --------------------------------------------------------------------------------------------

type Status struct {
	BaseEntity
	CPU int `json:"cpu"` // CPU usage value
	RAM int `json:"ram"` // RAM usage value
}

func (a Status) TABLE() string { return "status" }
func (a Status) NAME() string  { return fmt.Sprintf("%s: CPU:%d RAM:%d", a.ID(), a.CPU, a.RAM) }

func NewStatus() Entity {
	return &Status{}
}

func NewStatus1(cpu, ram int) Entity {
	return &Status{
		BaseEntity: BaseEntity{Id: NanoID(), CreatedOn: Now(), UpdatedOn: Now()},
		CPU:        cpu,
		RAM:        ram,
	}
}

// endregion

// region Status message -----------------------------------------------------------------------------------------------

type StatusMessage struct {
	BaseMessage
	Status *Status `json:"status"`
}

func (m *StatusMessage) Payload() any { return m.Status }

func NewStatusMessage() IMessage {
	return &StatusMessage{}
}

func newStatusMessage(topic string, status *Status, sessionId string) IMessage {
	message := &StatusMessage{
		Status: status,
	}
	message.MsgTopic = topic
	message.MsgOpCode = int(time.Now().Unix())
	message.MsgSessionId = sessionId
	message.MsgPayload = status
	return message
}

// endregion
