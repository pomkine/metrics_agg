package main

import (
	"github.com/pomkine/metrics_agg/internal/generator"
	"github.com/pomkine/metrics_agg/internal/generator/ds"
	"github.com/pomkine/metrics_agg/internal/mq"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	mQueue, err := mq.NewMQ(mq.Config{QueueSize: 10})
	if err != nil {
		log.Fatal(err)
	}

	mQueue.Start()

	publishFn := func(data ds.Data) {
		if err := mQueue.Publish(data); err != nil {
			log.Println(err)
		}
	}

	ds1 := []*ds.DataSource{
		ds.NewDataSource(ds.Config{
			ID:            "ds-1",
			InitValue:     10,
			MaxChangeStep: 5,
		}),
		ds.NewDataSource(ds.Config{
			ID:            "ds-2",
			InitValue:     10000,
			MaxChangeStep: 500,
		}),
	}

	ds2 := []*ds.DataSource{
		ds.NewDataSource(ds.Config{
			ID:            "ds-3",
			InitValue:     500000,
			MaxChangeStep: 5,
		}),
		ds.NewDataSource(ds.Config{
			ID:            "ds-4",
			InitValue:     10000000,
			MaxChangeStep: 50,
		}),
	}

	g1 := generator.NewGenerator(generator.Config{Timeout: 60, SendPeriod: 3}, ds1, publishFn)
	g2 := generator.NewGenerator(generator.Config{Timeout: 30, SendPeriod: 1}, ds2, publishFn)

	g1.Start()
	g2.Start()

	s1, err := mQueue.Subscribe("ds-1")
	if err != nil {
		log.Fatal(err)
	}
	s2, err := mQueue.Subscribe("ds-2")
	if err != nil {
		log.Fatal(err)
	}
	s3, err := mQueue.Subscribe("ds-3")
	if err != nil {
		log.Fatal(err)
	}
	s4, err := mQueue.Subscribe("ds-4")
	if err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	go consumer(s1)
	go consumer(s2)
	go consumer(s3)
	go consumer(s4)

	<-sigs
	stop(g1, g2, mQueue)
	log.Println("finished")
}

func stop(g1 *generator.Generator, g2 *generator.Generator, mQueue *mq.MQ) {
	<-g1.Stop()
	<-g2.Stop()
	<-mQueue.Stop()
}

func consumer(data <-chan ds.Data) {
	for data := range data {
		log.Println(data)
	}
}
