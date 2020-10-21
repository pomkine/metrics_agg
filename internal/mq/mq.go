package mq

import (
	"fmt"
	"github.com/pomkine/metrics_agg/internal/generator/ds"
	"log"
	"sync"
)

type Config struct {
	QueueSize int
}

type MQ struct {
	size int
	ops  chan func(subscribers)

	m       sync.RWMutex
	running bool

	stopSig  chan struct{}
	finished chan struct{}
}

func NewMQ(c Config) (*MQ, error) {
	if c.QueueSize <= 0 {
		return nil, fmt.Errorf("invalid queue size: %v", c.QueueSize)
	}
	return &MQ{
		size:     c.QueueSize,
		ops:      make(chan func(subscribers)),
		stopSig:  make(chan struct{}, 1),
		finished: make(chan struct{}, 1),
	}, nil
}

func (r *MQ) Start() {
	r.m.Lock()
	defer r.m.Unlock()
	if r.running {
		return
	} else {
		go r.loop()
		r.running = true
	}
}

func (r *MQ) Stop() <-chan struct{} {
	r.stopSig <- struct{}{}
	return r.finished
}

func (r *MQ) Publish(data ds.Data) error {
	if err := r.checkRunning(); err != nil {
		return err
	}
	r.ops <- func(s subscribers) {
		for _, sub := range s.byDataID(data.ID) {
			if len(sub) < r.size {
				sub <- data
			} else {
				log.Printf("dropped [%v]\n", data)
			}
		}
	}
	return nil
}

func (r *MQ) checkRunning() error {
	r.m.RLock()
	defer r.m.RUnlock()
	if !r.running {
		return fmt.Errorf("queue not running")
	}
	return nil
}

func (r *MQ) Subscribe(dataID string) (<-chan ds.Data, error) {
	if err := r.checkRunning(); err != nil {
		return nil, err
	}

	result := make(chan (<-chan ds.Data), 1)
	r.ops <- func(s subscribers) {
		result <- s.getDataChannelFor(dataID, r.size)
		return
	}
	return <-result, nil
}

func (r *MQ) loop() {
	var subs subscribers
	subs = make(map[string][]chan ds.Data)
	for {
		select {
		case op := <-r.ops:
			op(subs)
		case <-r.stopSig:
			subs.close()
			r.m.Lock()
			r.running = false
			r.m.Unlock()
			log.Println("queue stopped")
			r.finished <- struct{}{}
			return
		}
	}
}

type subscribers map[string][]chan ds.Data

func (s subscribers) getDataChannelFor(dataID string, size int) <-chan ds.Data {
	out := make(chan ds.Data, size)
	group, ok := s[dataID]
	if !ok {
		s[dataID] = []chan ds.Data{out}
	} else {
		group = append(group, out)
	}
	return out
}

func (s subscribers) byDataID(dataID string) []chan ds.Data {
	return s[dataID]
}

func (s subscribers) close() {
	for _, subs := range s {
		for _, sub := range subs {
			close(sub)
		}
	}
}
