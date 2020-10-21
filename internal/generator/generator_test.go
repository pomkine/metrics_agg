package generator

import (
	"github.com/magiconair/properties/assert"
	"github.com/pomkine/metrics_agg/internal/generator/ds"
	"testing"
	"time"
)

func TestGenerate_TillTimeout(t *testing.T) {
	sources := []*ds.DataSource{
		ds.NewDataSource(ds.Config{
			ID:            "ds-1",
			InitValue:     100,
			MaxChangeStep: 50,
		}),
		ds.NewDataSource(ds.Config{
			ID:            "ds-2",
			InitValue:     10000,
			MaxChangeStep: 5000,
		}),
	}
	timeout := 7
	cfg := Config{
		Timeout:    timeout,
		SendPeriod: 2,
	}
	var data []ds.Data
	onNewData := func(d ds.Data) {
		data = append(data, d)
	}
	sut := NewGenerator(cfg, sources, onNewData)
	sut.Start()
	<-time.NewTicker(7 * time.Second).C
	assert.Equal(t, len(data), 6)
}

func TestGenerate_Stops(t *testing.T) {
	sources := []*ds.DataSource{
		ds.NewDataSource(ds.Config{
			ID:            "ds-1",
			InitValue:     100,
			MaxChangeStep: 50,
		}),
	}
	timeout := 50
	cfg := Config{
		Timeout:    timeout,
		SendPeriod: 2,
	}
	var data []ds.Data
	onNewData := func(d ds.Data) {
		data = append(data, d)
	}
	sut := NewGenerator(cfg, sources, onNewData)
	sut.Start()
	<-time.NewTicker(5 * time.Second).C
	<-sut.Stop()
	assert.Equal(t, len(data), 2)
}
