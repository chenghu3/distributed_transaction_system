package client

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"

	"../shared"
)

// ServerMap : harded coded server address for client
var ServerMap = map[string]string{
	"A": "sp19-cs425-g10-01.cs.illinois.edu:9000",
	"B": "sp19-cs425-g10-01.cs.illinois.edu:9001",
	"C": "sp19-cs425-g10-01.cs.illinois.edu:9002",
	"D": "sp19-cs425-g10-01.cs.illinois.edu:9003",
	"E": "sp19-cs425-g10-01.cs.illinois.edu:9004"}

// Client Node
// No mutex for client node, most operations are single-thread, except for update queue, which is a thread-safe struct
type Client struct {
	Indentifier          string
	IsTransacting        bool
	IsAborted            bool
	TransactionCount     int
	Commands             *shared.CommandQueue
	TentativeWrite       map[string]map[string]string
	ReadLockSet          *shared.StringSet
	BlockedOperationChan chan bool
}

func newClient(name string) *Client {
	c := new(Client)
	c.Indentifier = shared.GetLocalIP() + name
	c.IsTransacting = false
	c.IsAborted = false
	c.TransactionCount = 1
	c.Commands = shared.NewQueue()
	c.TentativeWrite = make(map[string]map[string]string)
	c.ReadLockSet = shared.NewSet()
	return c
}

// StartClient : start client program that read stdin and make RPC calls
func StartClient(name string) {
	// Initalization
	client := newClient(name)
	in := bufio.NewReader(os.Stdin)
	// Keep reading stdin, add command to queue
	go func() {
		for {
			command, _, err := in.ReadLine()
			if err != nil {
				log.Fatal(err)
			}
			if string(command) == "ABORT" {
				// potential race condition
				handleAbort(client, false)
			} else {
				client.Commands.Push(string(command))
			}
		}
	}()
	// While loop handle command
	for {
		if !client.Commands.IsEmpty() {
			rawCommand := client.Commands.Pop()
			command := strings.Split(rawCommand, " ")
			action := command[0]
			switch action {
			case "BEGIN":
				handleBegin(client)
			case "SET":
				handleSet(client, command)
			case "GET":
				handleGet(client, command)
			case "COMMIT":
				handleCommit(client)
			default:
				fmt.Println("Invalid command: " + rawCommand)
			}
		}
	}
}

func handleBegin(client *Client) {
	client.IsTransacting = true
	fmt.Println("OK")
	// TODO: talk to coordinator
}

func handleSet(client *Client, command []string) {
	if client.IsAborted {
		fmt.Println("Abort")
	} else if !client.IsTransacting {
		fmt.Println("Transaction is not initiated.")
	} else if len(command) != 3 {
		fmt.Println("Invalid command: " + strings.Join(command, " "))
	} else {
		// Parse Command
		server := strings.Split(command[1], ".")[0]
		key := strings.Split(command[1], ".")[1]
		value := command[2]
		// check pass in server value
		if _, present := ServerMap[server]; !present {
			fmt.Println("System doesn't have requested server!")
			return
		}
		// Check tentiveWrite
		if _, present := client.TentativeWrite[server]; !present {
			client.TentativeWrite[server] = make(map[string]string)
		}
		// if key is present is our local tentiveWrite, means client already has the write lock of that object,
		// no need to make RPC call, just write to local tentiveWrite
		if _, present := client.TentativeWrite[server][key]; present {
			client.TentativeWrite[server][key] = value
			fmt.Println("OK")
		} else {
			// Make Synchronous RPC call to acquire lock, block client code until receive apply
			transactionID := client.Indentifier + strconv.Itoa(client.TransactionCount)
			// Blocking call
			reply := makeRPCRequest("TryPut", server, key, value, transactionID)
			// Write lock is granted
			if reply == "SUCCESS" {
				client.TentativeWrite[server][key] = value
				fmt.Println("OK")
			} else if reply == "ABORT" {
				handleAbort(client, false)
			} else {
				fmt.Println("Unknown server reply: " + reply)
			}
		}
	}
}

func handleGet(client *Client, command []string) {
	if client.IsAborted {
		fmt.Println("Abort")
	} else if !client.IsTransacting {
		fmt.Println("Transaction is not initiated.")
	} else if len(command) != 2 {
		fmt.Println("Invalid command: " + strings.Join(command, " "))
	} else {
		// Parse Command
		server := strings.Split(command[1], ".")[0]
		key := strings.Split(command[1], ".")[1]
		// check pass in server value
		if _, present := ServerMap[server]; !present {
			fmt.Println("System doesn't have requested server!")
			return
		}
		// if key is present is our local tentiveWrite, means client already has the write lock of that object,
		// no need to make RPC call, just return the current value in local storage
		if v, present := client.TentativeWrite[server][key]; present {
			fmt.Println(command[1] + " = " + v)
		} else {
			// Make Synchronous RPC call to acquire lock, block client code until receive apply
			transactionID := client.Indentifier + strconv.Itoa(client.TransactionCount)
			// Blocking call
			reply := makeRPCRequest("Read", server, key, "", transactionID)
			// Read lock is granted
			if strings.HasPrefix(reply, "SUCCESS") {
				client.ReadLockSet.SetAdd(server + "." + key)
				content := strings.Split(reply, " ")
				fmt.Println(content[1] + " = " + content[2])
			} else if strings.HasPrefix(reply, "ABORT") {
				handleAbort(client, false)
			} else if strings.HasPrefix(reply, "NOT FOUND") {
				fmt.Println("NOT FOUND")
				handleAbort(client, true)
			} else {
				fmt.Println("Unknown server reply: " + reply)
			}
		}
	}
}

// 1. Actually write 2. Release write lock 3. Release read lock and Clear up 4. increment transactionCount
func handleCommit(client *Client) {
	client.IsTransacting = false
	for server, storage := range client.TentativeWrite {
		for key, value := range storage {
			transactionID := client.Indentifier + strconv.Itoa(client.TransactionCount)
			// Blocking call, actually write to server
			makeRPCRequest("Put", server, key, value, transactionID)
			// release write lock
			// TODO: consider just it in PUT?
			makeRPCRequest("Release", server, key, "", transactionID)
		}
	}
	clearUpAndReleaseRead(client)
	client.TransactionCount++
	fmt.Println("COMMIT OK")
}

// NOT FOUND do not need to print "ABORTED"
// User Abort: talk to all server to release lock, clear all tentative write
// 				if there is any blocking request, server will send Abort mesg to unblock
// Serve Abort:
func handleAbort(client *Client, isNotFound bool) {
	// TODO: race
	// Abort is in action, do nothing
	if client.IsAborted {
		return
	}
	client.IsTransacting = false
	client.IsAborted = true
	for server, storage := range client.TentativeWrite {
		for key := range storage {
			transactionID := client.Indentifier + strconv.Itoa(client.TransactionCount)
			// release write lock
			makeRPCRequest("Release", server, key, "", transactionID)
		}
	}
	clearUpAndReleaseRead(client)
	if !isNotFound {
		fmt.Println("ABORTED")
	}
}

// clear up client local storage and release read lock for the case of commit and abort
func clearUpAndReleaseRead(client *Client) {
	// release all readLock
	for _, lockInfo := range client.ReadLockSet.SetToArray() {
		transactionID := client.Indentifier + strconv.Itoa(client.TransactionCount)
		server := strings.Split(lockInfo, ".")[0]
		key := strings.Split(lockInfo, ".")[1]
		makeRPCRequest("Release", server, key, "", transactionID)
	}
	// clear tentative write, readLockInfo and commandQueue
	client.TentativeWrite = make(map[string]map[string]string)
	client.ReadLockSet = shared.NewSet()
	client.Commands.Clear()
	// TODO: Clear up to coordinator
}

func makeRPCRequest(action string, server string, key string, value string, transactionID string) string {
	serverAddrr := ServerMap[server]
	rpcClient, err := rpc.Dial("tcp", serverAddrr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	args := &shared.Args{Key: key, Value: value, TransactionID: transactionID}
	var reply string
	switch action {
	case "TryPut":
		err = rpcClient.Call("Server.WriterLock", args, &reply)
	case "Put":
		err = rpcClient.Call("Server.Put", args, &reply)
	case "Read":
		err = rpcClient.Call("Server.Read", args, &reply)
	case "Release":
		err = rpcClient.Call("Server.ReleaseLock", args, &reply)
	default:
		fmt.Println("Unknown rpc request type: " + action)
	}
	if err != nil {
		log.Fatal("Server error:", err)
	}
	rpcClient.Close()
	return reply
}
