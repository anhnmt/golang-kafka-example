package logger

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/bytedance/sonic"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/anhnmt/golang-kafka-example/pkg/config"
)

func New(cfg config.Log) {
	var writer []io.Writer

	// UNIX Time is faster and smaller than most timestamps
	if cfg.Format == "json" {
		writer = append(writer, os.Stdout)
	} else {
		writer = append(writer, &zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
			NoColor:    false,
		})
	}

	if cfg.File != "" {
		writer = append(writer, &lumberjack.Logger{
			Filename:   cfg.File,
			MaxSize:    cfg.MaxSize, // megabytes
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge, // days
		})
	}

	level, err := zerolog.ParseLevel(cfg.Level)
	if err == nil {
		zerolog.SetGlobalLevel(level)
	}

	zerolog.TimeFieldFormat = time.RFC3339
	zerolog.InterfaceMarshalFunc = sonic.Marshal

	// Caller Marshal Function
	zerolog.CallerMarshalFunc = func(_ uintptr, file string, line int) string {
		return filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	log.Logger = zerolog.
		New(zerolog.MultiLevelWriter(writer...)).
		With().
		Timestamp().
		Caller().
		Logger()
}
