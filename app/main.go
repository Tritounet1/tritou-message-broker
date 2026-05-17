package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

/*
Test:

"Hello from netcat" | nc localhost 8090

Example code :
https://gobyexample.com/tcp-server
*/

func main() {

	// `net.Listen` starts the server on the given network
	// (TCP) and address (port 8090 on all interfaces).
	listener, err := net.Listen("tcp", ":8090")
	if err != nil {
		log.Fatal("Error listening:", err)
	}

	// Close the listener to free the port
	// when the application exits.
	defer listener.Close()

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
	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Printf("Server write error: %v", err)
	}
}
