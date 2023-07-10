package test

import (
	"fmt"
	. "github.com/go-yaaf/yaaf-common/entity"
	. "github.com/go-yaaf/yaaf-common/messaging"
	"github.com/go-yaaf/yaaf-common/utils/binary"
	"time"
)

// region Status data Model --------------------------------------------------------------------------------------------

type Status struct {
	BaseEntity
	CPU int `json:"cpu"` // CPU usage value
	RAM int `json:"ram"` // RAM usage value
}

func (a *Status) TABLE() string { return "status" }
func (a *Status) NAME() string  { return fmt.Sprintf("%s: CPU:%d RAM:%d", a.ID(), a.CPU, a.RAM) }

func (a *Status) MarshalBinary() (data []byte, err error) {
	w := binary.NewWriter()
	w.String(a.Id).Timestamp(a.CreatedOn).Timestamp(a.UpdatedOn).Int(a.CPU).Int(a.RAM)
	return w.GetBytes(), nil
}

func (a *Status) UnmarshalBinary(data []byte) (e error) {
	r := binary.NewReader(data)
	if a.Id, e = r.String(); e != nil {
		return e
	}
	if a.CreatedOn, e = r.Timestamp(); e != nil {
		return e
	}
	if a.UpdatedOn, e = r.Timestamp(); e != nil {
		return e
	}
	if a.CPU, e = r.Int(); e != nil {
		return e
	}
	if a.RAM, e = r.Int(); e != nil {
		return e
	}
	return nil
}

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

func (m *StatusMessage) MarshalBinary() (data []byte, err error) {
	w := binary.NewWriter()
	w.String(m.MsgTopic).Int(m.MsgOpCode).String(m.MsgVersion).String(m.MsgAddressee).String(m.MsgSessionId)

	if bytes, er := m.Status.MarshalBinary(); er != nil {
		return nil, er
	} else {
		w.Object(&bytes)
	}

	return w.GetBytes(), nil
}

func (m *StatusMessage) UnmarshalBinary(data []byte) (e error) {
	r := binary.NewReader(data)
	if m.MsgTopic, e = r.String(); e != nil {
		return e
	}
	if m.MsgOpCode, e = r.Int(); e != nil {
		return e
	}
	if m.MsgVersion, e = r.String(); e != nil {
		return e
	}
	if m.MsgAddressee, e = r.String(); e != nil {
		return e
	}
	if m.MsgSessionId, e = r.String(); e != nil {
		return e
	}

	if bytes, er := r.Object(); er != nil {
		return er
	} else {
		m.Status = &Status{}
		if e = m.Status.UnmarshalBinary(bytes); e != nil {
			return e
		}
	}
	return nil
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
	return message
}

// endregion
