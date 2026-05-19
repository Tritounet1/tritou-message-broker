package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func runClient() {
	conn, err := net.Dial("tcp", "localhost:8090")
	if err != nil {
		fmt.Printf("Connect error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Send a message: ")
		message, _ := reader.ReadString('\n')
		fmt.Fprintf(conn, message)

		response, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Printf("Server error: %v\n", err)
			return
		}
		fmt.Printf("Server says: %s", response)
	}
}
