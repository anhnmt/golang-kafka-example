package kafka

import (
	"errors"
)

var (
	errUnknownBroker = errors.New("unknown broker")
	errUnknownTopic  = errors.New("unknown topic")
	errUnknownGroup  = errors.New("unknown group")
)
