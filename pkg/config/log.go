package config

type Log struct {
	Format string `mapstructure:"format" defaultvalue:"text"`
	Level  string `mapstructure:"level" defaultvalue:"info"`

	// Log file
	File       string `mapstructure:"file" `
	MaxSize    int    `mapstructure:"max_size" defaultvalue:"100"` // MB
	MaxBackups int    `mapstructure:"max_backups" defaultvalue:"5"`
	MaxAge     int    `mapstructure:"max_age" defaultvalue:"28"` // days
}
