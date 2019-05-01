package main

import (
	"fmt"
	"os"

	"./src/client"
	"./src/coordinator"
	"./src/server"
)

func main() {
	args := os.Args
	if len(args) <= 2 {
		fmt.Println("Usage error: specify server/client info")
		os.Exit(1)
	}
	switch args[1] {
	case "server":
		if len(args) != 4 {
			fmt.Println("Usage error: ./mp3 server name port")
			os.Exit(1)
		}
		server.StartServer(args[2], args[3])
	case "client":
		if len(args) != 3 {
			fmt.Println("Usage error: ./mp3 client name")
			os.Exit(1)
		}
		client.StartClient(args[2])
	case "coordinator":
		if len(args) != 3 {
			fmt.Println("Usage error: ./mp3 coordinator port")
			os.Exit(1)
		}
		coordinator.StartCoordinator(args[2])
	default:
		fmt.Println("Usage error: Unknow type")
		os.Exit(1)
	}
}
