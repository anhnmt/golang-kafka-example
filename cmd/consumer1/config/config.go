package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/anhnmt/golang-kafka-example/pkg/config"
)

type Config struct {
	Log   config.Log   `mapstructure:"log"`
	Kafka config.Kafka `mapstructure:"kafka"`
}

func New() (Config, error) {
	cfg := Config{}

	dir, err := os.Getwd()
	if err != nil {
		return cfg, fmt.Errorf("getwd error: %w", err)
	}

	path := fmt.Sprintf("%s/%s", dir, "config.yml")
	err = config.Load(filepath.ToSlash(path), &cfg)
	if err != nil {
		return cfg, fmt.Errorf("read config error: %w", err)
	}

	return cfg, nil
}
