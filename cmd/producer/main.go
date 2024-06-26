package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	producer, err := kafka.NewProducer(ctx, cfg.Kafka)
	if err != nil {
		panic(fmt.Errorf("failed new kafka producer: %w", err))
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		for {
			str := fmt.Sprintf("hello world %s", time.Now().Format(time.RFC3339))
			log.Info().Msg(str)

			record := &kgo.Record{
				Topic: "test1",
				Value: []byte(str),
			}

			err = producer.SendRecord(ctx, record)
			if err != nil {
				return
			}

			time.Sleep(10 * time.Second)
		}
	}()

	select {
	case v := <-quit:
		log.Info().Any("v", v).Msg("signal.Notify")
	case done := <-ctx.Done():
		log.Info().Any("done", done).Msg("ctx.Done")
	}

	producer.Close()

	log.Info().Msg("Gracefully shutting down")
}
