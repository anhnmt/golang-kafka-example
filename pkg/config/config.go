package config

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

// err := config.Load("config.yml", &cfg)
//
//	if err != nil {
//	    ...
//	}
func Load(path string, cfg interface{}) error {
	viper.SetConfigFile(path)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		var configFileNotFoundError viper.ConfigFileNotFoundError
		if !errors.As(err, &configFileNotFoundError) {
			return fmt.Errorf("fatal error config file: %w", err)
		}
	}

	bindValues(cfg)

	err = viper.Unmarshal(cfg)
	if err != nil {
		return fmt.Errorf("unable to decode into struct, %v", err)
	}

	viper.WatchConfig()
	return nil
}

func bindValues(iface interface{}, parts ...string) {
	ift := reflect.TypeOf(iface)
	if ift != nil && ift.Kind() == reflect.Ptr {
		ift = ift.Elem()
	}

	ifv := reflect.ValueOf(iface)
	if ifv.Kind() == reflect.Ptr {
		ifv = ifv.Elem()
	}

	processField(ifv, ift, parts)
}

func processField(v reflect.Value, t reflect.Type, parts []string) {
	for i := 0; i < t.NumField(); i++ {
		fieldVal := v.Field(i)
		fieldType := t.Field(i)

		tag, ok := fieldType.Tag.Lookup("mapstructure")
		if !ok {
			continue
		}

		if fieldVal.Kind() == reflect.Struct {
			processField(fieldVal, fieldType.Type, append(parts, tag))
		} else {
			key := strings.Join(append(parts, tag), "_")
			viper.BindEnv(key)

			if defaultValue, ok := fieldType.Tag.Lookup("defaultvalue"); ok {
				viper.SetDefault(key, defaultValue)
			}
		}
	}
}
