package main

import (
	"github.com/go-yaaf/yaaf-common/logger"
	"github.com/go-yaaf/yaaf-common/messaging"
	"sync"
	"time"
)

type StatusAggregator struct {
	mq       messaging.IMessageBus
	name     string
	topic    string
	error    error
	duration time.Duration
	interval time.Duration

	status *Status    // Aggregated status
	count  int        // Message count per tie window
	locker sync.Mutex // Safeguard
}

// NewStatusAggregator is a factory method
func NewStatusAggregator(bus messaging.IMessageBus) *StatusAggregator {
	return &StatusAggregator{
		mq:       bus,
		name:     "demo",
		topic:    "status",
		duration: time.Hour,
		interval: time.Second,
	}
}

// Name configure consumer name
func (p *StatusAggregator) Name(name string) *StatusAggregator {
	p.name = name
	return p
}

// Topic configure message topic
func (p *StatusAggregator) Topic(topic string) *StatusAggregator {
	p.topic = topic
	return p
}

// Duration configure for how long the consumer will run
func (p *StatusAggregator) Duration(duration time.Duration) *StatusAggregator {
	p.duration = duration
	return p
}

// Interval configure the time window interval for aggregation
func (p *StatusAggregator) Interval(interval time.Duration) *StatusAggregator {
	p.interval = interval
	return p
}

// GetError return error
func (p *StatusAggregator) GetError() error {
	return p.error
}

// Start the consumer
func (p *StatusAggregator) Start(wg *sync.WaitGroup) {
	if subscriber, err := p.mq.Subscribe("status-aggregator", NewStatusMessage, p.processMessage, p.topic); err != nil {
		p.error = err
	} else {
		logger.Info("subscriber: %s is running", subscriber)
		go p.run(wg)
	}
}

// This consumer aggregate average data for the provided time window
func (p *StatusAggregator) processMessage(message messaging.IMessage) bool {
	p.locker.Lock()
	sm := message.(*StatusMessage)

	if p.status == nil {
		p.status = &Status{
			CPU: sm.Status.CPU,
			RAM: sm.Status.RAM,
		}
		p.status.CreatedOn = sm.Status.CreatedOn
		p.status.UpdatedOn = sm.Status.UpdatedOn
		p.count = 1
	} else {
		// Sum values
		p.count += 1
		p.status.CPU = p.status.CPU + sm.Status.CPU
		p.status.RAM = p.status.RAM + sm.Status.RAM
		p.status.UpdatedOn = sm.Status.CreatedOn
	}
	p.locker.Unlock()
	return true
}

// Send aggregated status to the log
func (p *StatusAggregator) logAggregatedStatus() {
	p.locker.Lock()
	if p.status != nil {
		// Calculate average
		p.status.CPU = p.status.CPU / p.count
		p.status.RAM = p.status.RAM / p.count
		logger.Info("[%s] %s", p.name, p.status.NAME())
	}
	p.status = nil
	p.count = 0
	p.locker.Unlock()
}

// Run starts the time window collector
func (p *StatusAggregator) run(wg *sync.WaitGroup) {

	// Run publisher until timeout and push status message every time interval
	after := time.After(p.duration)
	for {
		select {
		case _ = <-time.Tick(p.interval):
			p.logAggregatedStatus()
		case <-after:
			if wg != nil {
				wg.Done()
			}
			return
		}
	}
}
