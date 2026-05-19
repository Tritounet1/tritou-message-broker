package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"tidy/src/models"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var (
	db *gorm.DB
)

type Subscriber struct {
	topicName string
	client    net.Conn
}

var subscribers []Subscriber

func runServer() {

	// `net.Listen` starts the server on the given network
	// (TCP) and address (port 8090 on all interfaces).
	listener, err := net.Listen("tcp", ":8090")
	if err != nil {
		log.Fatal("Error listening:", err)
	}

	// Close the listener to free the port
	// when the application exits.
	defer listener.Close()

	loadDatabase()

	println("Server is starting on port 8090.")

	// Loop indefinitely to accept new client connections.
	for {
		// Wait for a connection.
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting conn:", err)
			continue
		}

		// We use a goroutine here to handle the connection
		// so that the main loop can continue accepting more
		// connections.
		go handleConnection(conn)
	}
}

// `handleConnection` handles a single client connection,
// reading one line of text from the client and returning a response.
func handleConnection(conn net.Conn) {
	// Closing the connection releases resources when
	// we are finished interacting with the client.
	defer conn.Close()

	// Use `bufio.NewReader` to read one line of data
	// from the client (terminated by a newline).
	for {
		reader := bufio.NewReader(conn)
		message, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Read error: %v", err)
			return
		}

		// Create and send a response back to the client,
		// demonstrating two-way communication.
		ackMsg := strings.ToUpper(strings.TrimSpace(message))
		response := fmt.Sprintf("ACK: %s\n", ackMsg)
		fmt.Println("Client message : ", strings.TrimSpace(message))

		if strings.HasPrefix(message, "CREATE_TOPIC") {
			topicName := strings.ReplaceAll(message, "CREATE_TOPIC ", "")
			ctx := context.Background()
			err = gorm.G[models.Topic](db).Create(ctx, &models.Topic{Name: topicName})
		} else if strings.HasPrefix(message, "PUBLISH") {

		} else if strings.HasPrefix(message, "SUBSCRIBE") {
			topicName := strings.ReplaceAll(message, "CREATE_TOPIC ", "")
			subscribers = append(subscribers, Subscriber{
				topicName: topicName,
				client:    conn,
			})
		} else if strings.HasPrefix(message, "ACK") {

		}

		_, err = conn.Write([]byte(response))
		if err != nil {
			log.Printf("Server write error: %v", err)
		}
	}
}

func loadDatabase() {
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// ctx := context.Background()

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
