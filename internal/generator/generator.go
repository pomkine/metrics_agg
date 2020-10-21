package generator

import (
	"github.com/pomkine/metrics_agg/internal/generator/ds"
	"log"
	"time"
)

type Config struct {
	Timeout    int
	SendPeriod int
}

type Generator struct {
	c           Config
	dataSources []*ds.DataSource
	onNewData   func(ds.Data)

	stopSig  chan struct{}
	finished chan struct{}
}

func NewGenerator(c Config, dataSources []*ds.DataSource, onNewData func(ds.Data)) *Generator {
	return &Generator{
		c:           c,
		dataSources: dataSources,
		onNewData:   onNewData,
		stopSig:     make(chan struct{}, 1),
		finished:    make(chan struct{}, 1),
	}
}

func (g *Generator) Stop() <-chan struct{} {
	g.stopSig <- struct{}{}
	return g.finished
}

func (g *Generator) Start() {
	go func() {
		emitNewData := time.NewTicker(time.Duration(g.c.SendPeriod) * time.Second)
		finish := time.NewTicker(time.Duration(g.c.Timeout) * time.Second)
		for {
			select {
			case <-emitNewData.C:
				for _, source := range g.dataSources {
					g.onNewData(source.GetData())
				}
			case <-finish.C:
				log.Println("generator finished")
				g.finished <- struct{}{}
				return
			case <-g.stopSig:
				log.Println("generator stopped")
				g.finished <- struct{}{}
				return
			}
		}
	}()
}
