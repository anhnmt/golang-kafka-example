package config

import (
	"errors"
	"runtime"
	"strings"

	"github.com/spf13/viper"

	"github.com/xdorro/golang-kafka-example/pkg/log"
)

// InitConfig initializes the config
func InitConfig() {
	// SetConfigFile explicitly defines the path, name and extension of the config file.
	// Viper will use this and not check any of the config paths.
	// .env - It will search for the .env file in the current directory
	viper.AddConfigPath(".")
	viper.SetConfigFile(".env")
	viper.SetConfigType("env")
	viper.AutomaticEnv()
	// Replace env key
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Set default values
	defaultConfig()

	// Init logger
	log.InitLogger()

	// Find and read the config file
	if err := viper.ReadInConfig(); err != nil {
		// Config file not found; ignore error if desired
		var notfound viper.ConfigFileNotFoundError
		if ok := errors.Is(err, notfound); !ok {
			log.Err(err).Msgf("Can't read the config file")
		}
	}

	log.Info().
		Str("goarch", runtime.GOARCH).
		Str("goos", runtime.GOOS).
		Str("version", runtime.Version()).
		Msg("Runtime information")
}
