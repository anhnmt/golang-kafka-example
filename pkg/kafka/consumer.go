package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzerolog"

	"github.com/anhnmt/golang-kafka-example/pkg/config"
)

var DefaultProcessTask = func(*kgo.Client, kgo.FetchTopicPartition) error {
	return nil
}

// ProcessTaskFunc is a function that consumes records
type ProcessTaskFunc func(*kgo.Client, kgo.FetchTopicPartition) error

type tp struct {
	t string
	p int32
}

type pconsumer struct {
	topic     string
	partition int32
	quit      chan struct{}
	done      chan struct{}
	recs      chan kgo.FetchTopicPartition
}

type Consumer struct {
	maxPollRecords int
	client         *kgo.Client
	processTask    ProcessTaskFunc
	consumers      map[tp]*pconsumer
}

func NewConsumer(ctx context.Context, cfg config.Kafka, consumerGroup string, consumeTopics []string) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errUnknownBroker
	}

	if len(consumeTopics) == 0 {
		return nil, errUnknownTopic
	}

	c := &Consumer{
		maxPollRecords: cfg.MaxPollRecords,
		processTask:    DefaultProcessTask,
		consumers:      make(map[tp]*pconsumer),
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(consumeTopics...),
		kgo.OnPartitionsAssigned(c.assigned),
		kgo.OnPartitionsRevoked(c.revoked),
		kgo.OnPartitionsLost(c.lost),
		kgo.AutoCommitMarks(),
		kgo.BlockRebalanceOnPoll(),
		kgo.RetryTimeout(15 * time.Second),
	}

	if cfg.AutoTopicCreation {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}

	if cfg.ConsumeRegex {
		opts = append(opts, kgo.ConsumeRegex())
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

	c.client = client
	return c, nil
}

func (c *Consumer) Close() {
	c.client.Close()
}

func (c *Consumer) ProcessTask(task ProcessTaskFunc) {
	c.processTask = task
}

func (c *Consumer) PollRecords(ctx context.Context) {
	for {
		// PollRecords is strongly recommended when using
		// BlockRebalanceOnPoll. You can tune how many records to
		// process at once (upper bound -- could all be on one
		// partition), ensuring that your processor loops complete fast
		// enough to not block a rebalance too long.
		fetches := c.client.PollRecords(ctx, c.maxPollRecords)
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			// Note: you can delete this block, which will result
			// in these errors being sent to the partition
			// consumers, and then you can handle the errors there.
			log.Panic().Err(err).
				Str("topic", topic).
				Int32("partition", partition).
				Msg("Failed polling")
		})

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			// Since we are using BlockRebalanceOnPoll, we can be
			// sure this partition consumer exists:
			//
			// * onAssigned is guaranteed to be called before we
			// fetch offsets for newly added partitions
			//
			// * onRevoked waits for partition consumers to quit
			// and be deleted before re-allowing polling.
			con := c.consumers[tp{p.Topic, p.Partition}]
			if con == nil {
				con = new(pconsumer)
				c.consumers[tp{p.Topic, p.Partition}] = con
			}

			con.recs <- p
		})

		c.client.AllowRebalance()
	}
}

func (pc *pconsumer) consume(cl *kgo.Client, fallback ProcessTaskFunc) {
	defer close(pc.done)
	log.Info().Msgf("Starting, topic: %s, partition: %d\n", pc.topic, pc.partition)
	defer log.Info().Msgf("Killing, topic: %s, partition: %d\n", pc.topic, pc.partition)

	for {
		select {
		case <-pc.quit:
			return
		case recs := <-pc.recs:
			func(recs kgo.FetchTopicPartition) {
				log.Info().Msgf("Fetch topic: %s, partition: %d", pc.topic, pc.partition)
				defer log.Info().Msgf("Commit topic: %s, partition: %d", pc.topic, pc.partition)

				if len(recs.Records) == 0 {
					return
				}

				if err := fallback(cl, recs); err != nil {
					log.Err(err).Msg("ProcessTask fallback error")
					return
				}

				cl.MarkCommitRecords(recs.Records...)
			}(recs)
		}
	}
}

func (c *Consumer) assigned(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &pconsumer{
				topic:     topic,
				partition: partition,

				quit: make(chan struct{}),
				done: make(chan struct{}),
				recs: make(chan kgo.FetchTopicPartition, c.maxPollRecords),
			}
			c.consumers[tp{topic, partition}] = pc
			go pc.consume(cl, c.processTask)
		}
	}
}

func (c *Consumer) revoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	c.killConsumers(ctx, revoked)
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		log.Err(err).Msg("Revoke commit failed")
	}
}

func (c *Consumer) lost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.killConsumers(ctx, lost)
	// Losing means we cannot commit: an error happened.
}

func (c *Consumer) killConsumers(_ context.Context, lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			t := tp{topic, partition}
			pc := c.consumers[t]
			close(pc.quit)
			delete(c.consumers, t)
			log.Info().Msgf("Waiting for work to finish topic: %s, partition: %d", topic, partition)
			wg.Add(1)
			go func(pc *pconsumer) {
				<-pc.done
				wg.Done()
			}(pc)
		}
	}
}
