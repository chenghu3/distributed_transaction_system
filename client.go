package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"./server"
)

func main() {
	// server
	arith := new(server.Arith)
	rpc.Register(arith)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":8888")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

	// client
	serverAddress := "sp19-cs425-g10-02.cs.illinois.edu"
	client, err := rpc.DialHTTP("tcp", serverAddress+":8888")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	// Synchronous call
	args := &server.Args{A: 7, B: 8}
	var reply int
	err = client.Call("Arith.Multiply", args, &reply)
	if err != nil {
		log.Fatal("arith error:", err)
	}
	fmt.Printf("Arith: %d*%d=%d", args.A, args.B, reply)
}
