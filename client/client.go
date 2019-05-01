package client

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"

	"../shared"
)

// ServerMap : harded coded server address for client
var ServerMap = map[string]string{
	"A": "sp19-cs425-g10-01.cs.illinois.edu:9000",
	"B": "sp19-cs425-g10-01.cs.illinois.edu:9001",
	"C": "sp19-cs425-g10-01.cs.illinois.edu:9002",
	"D": "sp19-cs425-g10-01.cs.illinois.edu:9003",
	"E": "sp19-cs425-g10-01.cs.illinois.edu:9004"}

const coordinatorAddr = "sp19-cs425-g10-02.cs.illinois.edu:9000"

// Client Node
// No mutex for client node, most operations are single-thread, except for update queue, which is a thread-safe struct
type Client struct {
	Indentifier      string
	IsTransacting    bool
	IsAborted        bool
	TransactionCount int
	Commands         *shared.CommandQueue
	TentativeWrite   map[string]map[string]string
	ReadLockSet      *shared.StringSet
	lock             sync.RWMutex
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
				// Fix with protected IsAborted variable
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
		} else {
			<-client.Commands.NotifyChann
		}
	}
}

func handleBegin(client *Client) {
	client.IsTransacting = true
	client.lock.Lock()
	client.IsAborted = false
	client.lock.Unlock()
	transactionID := client.Indentifier + strconv.Itoa(client.TransactionCount)
	makeRPCRequestToCoordinator("BEGIN", transactionID)
	fmt.Println("OK")
}

func handleSet(client *Client, command []string) {
	client.lock.RLock()
	if client.IsAborted {
		fmt.Println("Transaction is aborted, Please restart a new Transaction")
		client.lock.RUnlock()
		return
	}
	client.lock.RUnlock()
	if !client.IsTransacting {
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
		if v, present := client.TentativeWrite[server][key]; present && v != "" {
			client.TentativeWrite[server][key] = value
			fmt.Println("OK")
		} else {
			// Create kv pair in TentativeWrite
			client.TentativeWrite[server][key] = ""
			// Make Synchronous RPC call to acquire lock, block client code until receive apply
			transactionID := client.Indentifier + strconv.Itoa(client.TransactionCount)
			// Blocking call
			reply := makeRPCRequest("TryPut", server, key, value, transactionID)
			// Write lock is granted
			if reply == "SUCCESS" {
				client.ReadLockSet.SetDelete(command[1])

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
	client.lock.RLock()
	if client.IsAborted {
		fmt.Println("Transaction is aborted, Please restart a new Transaction")
		client.lock.RUnlock()
		return
	}
	client.lock.RUnlock()
	if !client.IsTransacting {
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
		if v, present := client.TentativeWrite[server][key]; present && v != "" {
			fmt.Println(command[1] + " = " + v)
		} else {
			client.ReadLockSet.SetAdd(server + "." + key)
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
	client.lock.RLock()
	if client.IsAborted {
		fmt.Println("Transaction is aborted, Please restart a new Transaction")
		client.lock.RUnlock()
		return
	}
	client.lock.RUnlock()

	client.IsTransacting = false
	for server, storage := range client.TentativeWrite {
		for key, value := range storage {
			transactionID := client.Indentifier + strconv.Itoa(client.TransactionCount)
			fmt.Println("handleCommit: calling PUT RELEASE ")
			// Blocking call, actually write to server
			makeRPCRequest("Put", server, key, value, transactionID)
			fmt.Println("handleCommit: PUT returned")
			// release write lock
			makeRPCRequest("Release", server, key, "", transactionID)
			fmt.Println("handleCommit: RELEASE returned")
		}
	}
	clearUpAndReleaseRead(client)
	client.TransactionCount++
	fmt.Println("COMMIT OK")
}

// NOT FOUND do not need to print "ABORTED"
// User Abort: talk to all server to release lock, clear all tentative write
// 				if there is any blocking request, server will send Abort mesg to unblock
// Serve Abort: no race, happend in main thread
func handleAbort(client *Client, isNotFound bool) {
	client.lock.RLock()
	// Abort is in action, do nothing
	// This case only will happen when user abort because one request is blocked,
	// and later the blocked request receive abort mesg from server:
	//		case1: this happen during user abort, will be ignored
	// 		case2: after user abort, new transaction can't happend until this block call is resovled
	// 				and the abort mesg will be ignored as well
	if client.IsAborted {
		client.lock.RUnlock()
		return
	}
	client.lock.RUnlock()
	client.IsTransacting = false
	// change IsAborted, lock it
	client.lock.Lock()
	client.IsAborted = true
	client.lock.Unlock()
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
	// Clear up to coordinator
	transactionID := client.Indentifier + strconv.Itoa(client.TransactionCount)
	makeRPCRequestToCoordinator("REMOVE", transactionID)
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
		err = rpcClient.Call("Server.Write", args, &reply)
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

func makeRPCRequestToCoordinator(action string, transactionID string) string {
	rpcClient, err := rpc.Dial("tcp", coordinatorAddr)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	args := &shared.CoordinatorArgs{From: transactionID, To: []string{}}
	var reply string
	switch action {
	case "BEGIN":
		err = rpcClient.Call("Coordinator.AddTransaction", args, &reply)
	case "REMOVE":
		err = rpcClient.Call("Coordinator.RemoveTransaction", args, &reply)
	default:
		fmt.Println("Unknown Coordinator rpc request type: " + action)
	}
	if err != nil {
		log.Fatal("Coordinator error:", err)
	}
	rpcClient.Close()
	return reply
}
