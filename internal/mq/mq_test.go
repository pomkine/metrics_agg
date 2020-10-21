package mq

import (
	"github.com/pomkine/metrics_agg/internal/generator/ds"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMQ_Publish_SubscriberGetsData(t *testing.T) {
	mq, _ := NewMQ(Config{QueueSize: 10})
	sent := ds.Data{
		ID:    "test",
		Value: 100,
	}

	mq.Start()

	sub, err := mq.Subscribe("test")
	assert.NoError(t, err)

	err = mq.Publish(sent)
	assert.NoError(t, err)

	got := <-sub
	<-mq.Stop()
	assert.Equal(t, sent, got)
}

func TestMQ_Publish_FailsIfNotStarted(t *testing.T) {
	mq, _ := NewMQ(Config{QueueSize: 10})
	data := ds.Data{
		ID:    "test",
		Value: 100,
	}
	err := mq.Publish(data)
	assert.Error(t, err)
}

func TestMQ_Subscribe_FailsIfNotStarted(t *testing.T) {
	mq, _ := NewMQ(Config{QueueSize: 10})
	_, err := mq.Subscribe("test")
	assert.Error(t, err)
}

func TestMQ_New_FailsOnInvalidQueueSize(t *testing.T) {
	_, err := NewMQ(Config{QueueSize: 0})
	assert.Error(t, err)
}

func TestMQ_Start_ConsecutiveStartCallsWontTakeEffect(t *testing.T) {
	mq, _ := NewMQ(Config{QueueSize: 10})
	mq.Start()
	mq.Start()
}
