package config

type Kafka struct {
	Brokers           []string `mapstructure:"brokers"`
	Debug             bool     `mapstructure:"debug" defaultvalue:"true"`
	AutoTopicCreation bool     `mapstructure:"auto_topic_creation" defaultvalue:"true"`
	ConsumeRegex      bool     `mapstructure:"consume_regex"`
	ConsumerGroup     string   `mapstructure:"consumer_group" defaultvalue:"kafka_example"`
	MaxPollRecords    int      `mapstructure:"max_poll_records" defaultvalue:"5"`
	Topics            []string `mapstructure:"topics"`
}
