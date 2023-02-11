package test

import (
	ps "github.com/go-yaaf/yaaf-common-pubsub/gpubsub"
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"
)

type StatusLogger struct {
	uri   string
	name  string
	topic string
	error error
}

// NewStatusLogger is a factory method
func NewStatusLogger(uri string) *StatusLogger {
	return &StatusLogger{uri: uri, name: "demo", topic: "topic"}
}

// Name configure consumer name
func (p *StatusLogger) Name(name string) *StatusLogger {
	p.name = name
	return p
}

// Topic configure message channel (topic) name
func (p *StatusLogger) Topic(topic string) *StatusLogger {
	p.topic = topic
	return p
}

// Start the logger
func (p *StatusLogger) Start() {
	if mq, err := ps.NewPubSubMessageBus(p.uri); err != nil {
		logger.Error("error starting StatusLogger: %s", err.Error())
		p.error = err
	} else {
		if subscriber, er := mq.Subscribe(NewStatusMessage, p.processMessage, "StatusLogger", p.topic); er != nil {
			logger.Error(er.Error())
		} else {
			logger.Info("StatusAggregator Subscriber: %s", subscriber)
		}
	}
}

// GetError return error
func (p *StatusLogger) GetError() error {
	return p.error
}

// This consumer just print the message to the console
func (p *StatusLogger) processMessage(message messaging.IMessage) bool {
	sm := message.(*StatusMessage)
	logger.Debug("[%s] %s", p.name, sm.Status.NAME())
	return false
}
