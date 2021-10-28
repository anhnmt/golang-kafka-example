package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/xdorro/golang-kafka-example/handler"
	"github.com/xdorro/golang-kafka-example/model"
)

const (
	kafkaURL = "localhost:19092"
	topic    = "message-log"
	groupID  = "handler-group"
)

func main() {
	// create a new context
	ctx := context.Background()

	db, err := gorm.Open(
		mysql.Open("root:123456aA@@tcp(localhost:3306)/kafka?charset=utf8&parseTime=True&loc=Local"),
		&gorm.Config{},
	)
	if err != nil {
		log.Fatalf("gorm.Open(): %v", err)
	}

	// Migrate the schema
	if err = db.AutoMigrate(&model.Sync{}); err != nil {
		log.Fatalf("db.AutoMigrate(): %v", err)
	}

	// produce messages in a new go routine, since
	// both the produce and consume functions are
	// blocking
	go produce(ctx)
	consume(ctx, db)
}

func produce(ctx context.Context) {
	writer := handler.NewProducer(kafkaURL, topic)

	defer func(writer *kafka.Writer) {
		err := writer.Close()
		if err != nil {
			log.Fatalf("writer.Close(): %v", err)
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
			log.Fatalf("writer.WriteMessages(): %v", err)
		} else {
			fmt.Printf("produced at key:%v value:%v\n", key, string(msg.Value))
		}
		time.Sleep(5 * time.Second)
	}
}

func consume(ctx context.Context, db *gorm.DB) {
	reader := handler.NewConsumer(kafkaURL, topic, groupID)

	defer func(reader *kafka.Reader) {
		err := reader.Close()
		if err != nil {
			log.Fatalf("reader.Close(): %v", err)
		}
	}(reader)

	fmt.Println("start consuming ... !!")
	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("reader.ReadMessage(): %v", err)
		}

		key := string(m.Key)
		val := string(m.Value)
		db.Create(&model.Sync{Key: key, Value: val})
		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, key, val)
	}
}
