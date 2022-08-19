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
		kgo.ConsumerGroup("base-project"),
		kgo.ConsumeTopics("xdorro"),
		kgo.WithLogger(kzerolog.New(&log.Logger)),
		kgo.DisableAutoCommit(),
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

	// 2.) Consuming messages from a topic
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// All errors are retried internally when fetching, but non-retriable errors are
			// returned from polls so that users can notice and take action.
			panic(fmt.Sprint(errs))
		}

		// or a callback function.
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			// We can even use a second callback!
			p.EachRecord(func(record *kgo.Record) {
				err = cl.CommitRecords(ctx, record)
				if err != nil {
					return
				}

				fmt.Println(string(record.Value), "from a second callback!")
			})
		})
	}
}
