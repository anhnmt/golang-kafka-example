package model

import (
	"gorm.io/gorm"
)

type Sync struct {
	gorm.Model
	Key   string
	Value string
}
