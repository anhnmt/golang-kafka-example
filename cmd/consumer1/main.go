package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/anhnmt/golang-kafka-example/cmd/producer/config"
	"github.com/anhnmt/golang-kafka-example/pkg/kafka"
	"github.com/anhnmt/golang-kafka-example/pkg/logger"
)

func main() {
	cfg, err := config.New()
	if err != nil {
		panic(fmt.Errorf("failed get config: %w", err))
	}

	logger.New(cfg.Log)

	_, err = maxprocs.Set(maxprocs.Logger(log.Info().Msgf))
	if err != nil {
		panic(fmt.Errorf("failed set maxprocs: %w", err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Info().Msg("Starting producer")

	consumer, err := kafka.NewConsumer(ctx, cfg.Kafka, "group1", cfg.Kafka.Topics)
	if err != nil {
		panic(fmt.Errorf("failed new kafka: %w", err))
	}

	consumer.ProcessTask(func(cl *kgo.Client, recs kgo.FetchTopicPartition) error {
		for _, rec := range recs.Records {
			log.Info().
				Str("value", string(rec.Value)).
				Int64("offset", rec.Offset).
				Msg("Message")

			cl.MarkCommitRecords(rec)
		}

		return nil
	})

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go consumer.PollRecords(ctx)

	select {
	case v := <-quit:
		log.Info().Any("v", v).Msg("signal.Notify")
	case done := <-ctx.Done():
		log.Info().Any("done", done).Msg("ctx.Done")
	}

	consumer.Close()

	log.Info().Msg("Gracefully shutting down")
}
