package config

import (
	"github.com/spf13/viper"
)

// defaultConfig is the default configuration for the application.
func defaultConfig() {
	// APP
	viper.SetDefault("APP_PORT", 8088)
	viper.SetDefault("LOG_FILE_URL", "logs/data.log")
	// DATABASE
	viper.SetDefault("DB_URL", "mongodb://localhost:27017")
	viper.SetDefault("DB_NAME", "kafka")

	// KAFKA
	viper.SetDefault("KAFKA_URL", "localhost:9092")
}
