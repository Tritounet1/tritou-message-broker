package models

import "gorm.io/gorm"

type Topic struct {
	gorm.Model
	Name string
}
