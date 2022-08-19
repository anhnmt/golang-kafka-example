package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzerolog"

	"github.com/xdorro/golang-kafka-example/config"
	"github.com/xdorro/golang-kafka-example/pkg/log"
)

func main() {
	// Init config
	config.InitConfig()

	log.Infof("Hello World")

	brokers := strings.Split(viper.GetString("KAFKA_URL"), ",")

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
		kgo.WithLogger(kzerolog.New(&log.Logger)),
	)
	if err != nil {
		log.Err(err).Msg("unable to create client")
	}
	defer cl.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pingCtx, pingCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pingCancel()

	err = cl.Ping(pingCtx)
	if err != nil {
		log.Err(err).Msg("unable to ping")
		return
	}

	// 1.) Producing a message
	// // All record production goes through Produce, and the callback can be used
	// // to allow for synchronous or asynchronous production.

	// for i := 0; i < 5000; i++ {
	record := &kgo.Record{
		Topic: "xdorro",
		Value: []byte("bar"),
	}

	// Alternatively, ProduceSync exists to synchronously produce a batch of records.
	if err = cl.ProduceSync(ctx, record).FirstErr(); err != nil {
		fmt.Printf("record had a produce error while synchronously producing: %v\n", err)
	}
	// }

	cl.Close()
	log.Infof("Done")
}
