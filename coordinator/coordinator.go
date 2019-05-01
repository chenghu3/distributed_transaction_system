package coordinator

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"

	"../shared"
	"github.com/twmb/algoimpl/go/graph"
)

type Coordinator struct {
	Transactions map[string]graph.Node
	Graph        *graph.Graph
}

func NewCoordinator() *Coordinator {
	coordinator := new(Coordinator)
	coordinator.Transactions = make(map[string]graph.Node)
	coordinator.Graph = graph.New(graph.Directed)
	return coordinator
}

func (coordinator *Coordinator) AddTransaction(args *shared.CoordinatorArgs, reply *string) error {
	_, found := coordinator.Transactions[args.From]
	if !found {
		fmt.Println("Adding new transaction " + args.From)
		coordinator.Transactions[args.From] = coordinator.Graph.MakeNode()
	}
	return nil
}

// From one transaction to an array of transactions
func (coordinator *Coordinator) AddWaitEdge(args *shared.CoordinatorArgs, reply *string) error {
	_, foundFrom := coordinator.Transactions[args.From]
	if !foundFrom {
		return errors.New("AddWaitEdge: Transaction " + args.From + " not found in coordinator")
	}
	for _, to := range args.To {
		_, foundTo := coordinator.Transactions[to]
		if !foundTo {
			return errors.New("AddWaitEdge: Transaction " + to + " not found in coordinator")
		}
		fmt.Println("Adding edge from "+args.From, " to "+to)
		coordinator.Graph.MakeEdge(coordinator.Transactions[args.From], coordinator.Transactions[to])
	}

	return nil
}

func (coordinator *Coordinator) RemoveTransaction(args *shared.CoordinatorArgs, reply *string) error {
	node, found := coordinator.Transactions[args.From]
	if !found {
		return errors.New("RemoveTransaction: Transaction not found in coordinator")
	}
	fmt.Println("Removing transaction " + args.From)
	coordinator.Graph.RemoveNode(&node)
	delete(coordinator.Transactions, args.From)
	return nil
}

func (coordinator *Coordinator) DetectCycle(args *shared.CoordinatorArgs, reply *string) error {
	fmt.Println("Detecting Cycle")
	scc := coordinator.Graph.StronglyConnectedComponents()
	if len(scc) < len(coordinator.Transactions) {
		fmt.Println("Cycle detected")
		*reply = "CYCLE"
	} else {
		fmt.Println("No cycle")
		*reply = "NOCYCLE"
	}
	return nil
}

// StartCoordinator : Start server with serverID at port
func StartCoordinator(port string) {
	coordinator := NewCoordinator()
	rpc.Register(coordinator)

	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+port)
	if err != nil {
		log.Fatal("Error:", err)
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal("Error:", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Accept Error")
			continue
		}
		go rpc.ServeConn(conn)
	}
}
