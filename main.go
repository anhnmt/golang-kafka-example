package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/xdorro/golang-kafka-example/handler"
)

const (
	kafkaURL = "localhost:19092"
	topic    = "message-log"
	groupID  = "handler-group"
)

func main() {
	// create a new context
	ctx := context.Background()
	// produce messages in a new go routine, since
	// both the produce and consume functions are
	// blocking
	go produce(ctx)
	consume(ctx)
}

func produce(ctx context.Context) {
	writer := handler.NewProducer(kafkaURL, topic)

	defer func(writer *kafka.Writer) {
		err := writer.Close()
		if err != nil {
			log.Fatalf("handler writer closed: %v", err)
		}
	}(writer)

	fmt.Println("start producing ... !!")
	for i := 0; ; i++ {
		key := fmt.Sprintf("Key-%d", i)
		msg := kafka.Message{
			Key:   []byte(key),
			Value: []byte(fmt.Sprint(uuid.New())),
		}
		err := writer.WriteMessages(ctx, msg)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("produced at key:%v value:%v\n", key, string(msg.Value))
		}
		time.Sleep(5 * time.Second)
	}
}

func consume(ctx context.Context) {
	reader := handler.NewConsumer(kafkaURL, topic, groupID)

	defer func(reader *kafka.Reader) {
		err := reader.Close()
		if err != nil {
			log.Fatalf("handler reader closed: %v", err)
		}
	}(reader)

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}
