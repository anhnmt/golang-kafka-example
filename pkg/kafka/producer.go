package kafka

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzerolog"

	"github.com/anhnmt/golang-kafka-example/pkg/config"
)

type Producer struct {
	client *kgo.Client
}

func NewProducer(ctx context.Context, cfg config.Kafka) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errUnknownBroker
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RetryTimeout(15 * time.Second),
		kgo.ProducerBatchCompression([]kgo.CompressionCodec{kgo.NoCompression()}...),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	}

	if cfg.AutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if cfg.Debug {
		opts = append(opts, kgo.WithLogger(kzerolog.New(&log.Logger)))
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err = client.Ping(ctx); err != nil { // check connectivity to cluster
		return nil, err
	}

	p := &Producer{
		client: client,
	}

	return p, nil
}

func (p *Producer) Close() {
	p.client.Close()
}

func (p *Producer) SendRecord(ctx context.Context, record ...*kgo.Record) error {
	return p.ProduceSync(ctx, record...).FirstErr()
}

func (p *Producer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	p.client.Produce(ctx, r, promise)
}

func (p *Producer) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	return p.client.ProduceSync(ctx, rs...)
}
