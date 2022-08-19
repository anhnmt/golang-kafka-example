package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	consumerGroup := viper.GetString("KAFKA_GROUP")
	topic := viper.GetString("KAFKA_TOPIC")

	s := &splitConsume{
		consumers: make(map[tp]*pconsumer),
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.WithLogger(kzerolog.New(&log.Logger)),
		kgo.OnPartitionsAssigned(s.assigned),
		kgo.OnPartitionsRevoked(s.lost),
		kgo.OnPartitionsLost(s.lost),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
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

	s.poll(cl)
}

type tp struct {
	t string
	p int32
}

type pconsumer struct {
	cl        *kgo.Client
	topic     string
	partition int32

	quit chan struct{}
	done chan struct{}
	recs chan []*kgo.Record
}

func (pc *pconsumer) consume() {
	defer close(pc.done)
	fmt.Printf("Starting consume for  t %s p %d\n", pc.topic, pc.partition)
	defer fmt.Printf("Closing consume for t %s p %d\n", pc.topic, pc.partition)
	for {
		select {
		case <-pc.quit:
			return
		case recs := <-pc.recs:
			for _, rec := range recs {
				// time.Sleep(500 * time.Millisecond) // simulate work

				fmt.Printf("Some sort of work done, about to commit t %s p %d\n", pc.topic, pc.partition)

				err := pc.cl.CommitRecords(context.Background(), rec)
				if err != nil {
					fmt.Printf("Error when committing offsets to kafka err: %v t: %s p: %d offset %d\n", err, pc.topic, pc.partition, recs[len(recs)-1].Offset+1)
				}
			}
		}
	}
}

type splitConsume struct {
	// Using BlockRebalanceOnCommit means we do not need a mu to manage
	// consumers, unlike the autocommit normal example.
	consumers map[tp]*pconsumer
}

func (s *splitConsume) assigned(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &pconsumer{
				cl:        cl,
				topic:     topic,
				partition: partition,

				quit: make(chan struct{}),
				done: make(chan struct{}),
				recs: make(chan []*kgo.Record, 5),
			}
			tpic := tp{topic, partition}
			s.consumers[tpic] = pc
			go pc.consume()
		}
	}
}

// In this example, each partition consumer commits itself. Those commits will
// fail if partitions are lost, but will succeed if partitions are revoked. We
// only need one revoked or lost function (and we name it "lost").
func (s *splitConsume) lost(_ context.Context, cl *kgo.Client, lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			tpic := tp{topic, partition}
			pc := s.consumers[tpic]
			delete(s.consumers, tpic)
			close(pc.quit)
			fmt.Printf("waiting for work to finish t %s p %d\n", topic, partition)
			wg.Add(1)
			go func() { <-pc.done; wg.Done() }()
		}
	}
}

func (s *splitConsume) poll(cl *kgo.Client) {
	for {
		// PollRecords is strongly recommended when using
		// BlockRebalanceOnPoll. You can tune how many records to
		// process at once (upper bound -- could all be on one
		// partition), ensuring that your processor loops complete fast
		// enough to not block a rebalance too long.
		fetches := cl.PollRecords(context.Background(), 10000)
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachError(func(_ string, _ int32, err error) {
			// Note: you can delete this block, which will result
			// in these errors being sent to the partition
			// consumers, and then you can handle the errors there.
			panic(err)
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			tpic := tp{p.Topic, p.Partition}

			// Since we are using BlockRebalanceOnPoll, we can be
			// sure this partition consumer exists:
			//
			// * onAssigned is guaranteed to be called before we
			// fetch offsets for newly added partitions
			//
			// * onRevoked waits for partition consumers to quit
			// and be deleted before re-allowing polling.
			s.consumers[tpic].recs <- p.Records
		})
		cl.AllowRebalance()
	}
}
