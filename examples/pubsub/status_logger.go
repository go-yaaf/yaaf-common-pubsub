package main

import (
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"
)

type StatusLogger struct {
	mq    messaging.IMessageBus
	name  string
	topic string
	error error
}

// NewStatusLogger is a factory method
func NewStatusLogger(bus messaging.IMessageBus) *StatusLogger {
	return &StatusLogger{mq: bus, name: "demo", topic: "status"}
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
	if subscriber, err := p.mq.Subscribe("status-logger", NewStatusMessage, p.processMessage, p.topic); err != nil {
		p.error = err
	} else {
		logger.Info("subscriber: %s is running", subscriber)
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
