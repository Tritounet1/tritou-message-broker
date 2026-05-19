package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: go run . [client|server]")
		return
	}

	switch os.Args[1] {
	case "client":
		runClient()
	case "server":
		runServer()
	default:
		fmt.Println("unknown command")
	}
}
