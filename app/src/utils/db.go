package utils

import (
	"context"
	"tidy/src/models"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var (
	db *gorm.DB
)

func createTopic(topicName string) {
	ctx := context.Background()
	err := gorm.G[models.Topic](db).Create(ctx, &models.Topic{Name: topicName})
	if err != nil {
		// TODO: Trouver le bon moyen de print les erreurs, avec un errorHandler serait très bien. (middleware)
		println("Error while trying to create a topic : ", err.Error())
	}

}

func initDatabase() {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// ctx = context.Background()

	// Migrate the schema
	db.AutoMigrate(models.Topic{}, models.Service{})

	/*
		// Create
		err = gorm.G[Product](db).Create(ctx, &Product{Code: "D42", Price: 100})

		// Read
		product, err := gorm.G[Product](db).Where("id = ?", 1).First(ctx)       // find product with integer primary key
		products, err := gorm.G[Product](db).Where("code = ?", "D42").Find(ctx) // find product with code D42

		// Update - update product's price to 200
		err = gorm.G[Product](db).Where("id = ?", product.ID).Update(ctx, "Price", 200)
		// Update - update multiple fields
		err = gorm.G[Product](db).Where("id = ?", product.ID).Updates(ctx, Product{Code: "D42", Price: 100})

		// Delete - delete product
		err = gorm.G[Product](db).Where("id = ?", product.ID).Delete(ctx)
	*/
}
