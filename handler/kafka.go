package handler

import (
	"strings"

	"github.com/segmentio/kafka-go"
)

func NewProducer(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(kafkaURL),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func NewConsumer(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
}

// func main() {
// 	// get handler writer using environment variables.
// 	kafkaURL := "localhost:19092"
// 	topic := "handler-topic"
// 	writer := getKafkaWriter(kafkaURL, topic)
//
// 	defer func(writer *handler.Writer) {
// 		err := writer.Close()
// 		if err != nil {
// 			log.Fatalf("handler writer closed: %v", err)
// 		}
// 	}(writer)
//
// 	fmt.Println("start producing ... !!")
// 	for i := 0; ; i++ {
// 		key := fmt.Sprintf("Key-%d", i)
// 		msg := handler.Message{
// 			Key:   []byte(key),
// 			Value: []byte(fmt.Sprint(uuid.New())),
// 		}
// 		err := writer.WriteMessages(context.Background(), msg)
// 		if err != nil {
// 			fmt.Println(err)
// 		} else {
// 			fmt.Println("produced", key)
// 		}
// 		time.Sleep(1 * time.Second)
// 	}
// }
