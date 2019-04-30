package server

import (
	"errors"
	"sync"

	"../shared"
)

// Server Node
type Server struct {
	ID      string
	Objects map[string]Object
}

// NewServer : Server Node constructor
func NewServer(id string) *Server {
	server := new(Server)
	server.ID = id
	server.Objects = make(map[string]Object)
	return server
}

// Object : single object in server
type Object struct {
	Value        string
	Readers      *shared.StringSet // Transaction ID
	Writer       string            // Transaction ID
	RequestQueue []*LockRequest
	m            sync.Mutex
}

// NewObject : Server Object constructor
func NewObject(value string) *Object {
	object := new(Object)
	object.Value = value
	object.Readers = shared.NewSet()
	object.Writer = ""
	object.RequestQueue = make([]*LockRequest, 0)
	return object
}

// LockRequest : single request for requiring a lock
type LockRequest struct {
	Type          string // read, write, upgrade
	TransactionID string
	Channel       chan bool // granted, abort
}

// NewLockRequest : LockRequest constructor
func NewLockRequest(t string, tid string) *LockRequest {
	req := new(LockRequest)
	req.Type = t
	req.TransactionID = tid
	req.Channel = make(chan bool)
	return req
}

// ReleaseLock : release lock on object for transaction
func (server *Server) ReleaseLock(args *shared.Args, reply *string) error {
	obj, found := server.Objects[args.Key]
	if !found {
		return errors.New("ReleaseLock: Object key=" + args.Key + " not found")
	}
	isReader := obj.Readers.SetDelete(args.TransactionID)
	isWriter := obj.Writer == args.TransactionID
	if !isReader && !isWriter {
		return errors.New("ReleaseLock: Object key=" + args.Key + " not locked by transaction " + args.TransactionID)
	}
	if isWriter {
		obj.Writer = ""
	}
	*reply = "SUCCESS"
	// TODO: Grant lock to next request(s) in queue

	return nil
}

// WriterLock : Get Write lock of the corresponding object
func (server *Server) WriterLock(args *shared.Args, reply *string) error {
	obj, found := server.Objects[args.Key]

	// New object immediately gets writer access
	if !found {
		*reply = "SUCCESS"
		return nil
	}

	obj.m.Lock()
	// No Writer
	if obj.Writer == "" {
		// No reader/writer, immediately grant
		if obj.Readers.Size() == 0 {
			obj.Writer = args.TransactionID
			*reply = "SUCCESS"
			obj.m.Unlock()
		} else {
			// No writer, has readers
			// Client transaction is already reading,
			if obj.Readers.SetHas(args.TransactionID) {
				// Client transaction is the only reader
				if obj.Readers.Size() == 1 {
					// Immediately Promote
					obj.Writer = args.TransactionID
					obj.Readers.SetDelete(args.TransactionID)
					*reply = "SUCCESS"
					obj.m.Unlock()
				} else {
					// Client transaction is not the only reader
					// Wait until transaction is the only reader, then promote
					req := NewLockRequest("promote", args.TransactionID)
					obj.RequestQueue = append([]*LockRequest{req}, obj.RequestQueue...) // Prepend to queue
					obj.m.Unlock()
					// Wait for grant/abort
					ok := <-req.Channel
					if ok {
						*reply = "SUCCESS"
					} else {
						*reply = "ABORT"
					}
				}
			} else {
				// Client transaction is not reader, wait for releasing of all read locks
				req := NewLockRequest("write", args.TransactionID)
				obj.RequestQueue = append(obj.RequestQueue, req)
				obj.m.Unlock()
				// Wait for grant/abort
				ok := <-req.Channel
				if ok {
					*reply = "SUCCESS"
				} else {
					*reply = "ABORT"
				}
			}
		}
	} else {
		// Write lock is hold by other, wait for releasing of writer lock(No need to consider readers)
		req := NewLockRequest("write", args.TransactionID)
		obj.RequestQueue = append(obj.RequestQueue, req)
		obj.m.Unlock()
		// Wait for grant/abort
		ok := <-req.Channel
		if ok {
			*reply = "SUCCESS"
		} else {
			*reply = "ABORT"
		}
	}
	return nil
}

// Read : Get Read lock of the corresponding object and send back read value if read clock is acquired.
func (server *Server) Read(args *shared.Args, reply *string) error {
	obj, found := server.Objects[args.Key]

	if !found {
		*reply = "NOT FOUND"
		return nil
	}

	if obj.Writer != "" {
		// Writer is writing, append to queue

	} else {
		// if len(obj.RequestQueue) > 0
	}
	return nil
}

// Set : Perform the update (on commit)
// func (server *Server) Set(args *shared.Args, reply *string) error {

// 	return nil
// }

// func (server *Server) Commit(args *shared.Args, reply *string) error {

// 	return nil
// }

// func (server *Server) Abort(args *shared.Args, reply *string) error {

// 	return nil
// }

// type Quotient struct {
// 	Quo, Rem int
// }

// type Arith int

// func (t *Arith) Multiply(args *Args, reply *int) error {
// 	*reply = args.A * args.B
// 	return nil
// }

// func (t *Arith) Divide(args *Args, quo *Quotient) error {
// 	if args.B == 0 {
// 		return errors.New("divide by zero")
// 	}
// 	quo.Quo = args.A / args.B
// 	quo.Rem = args.A % args.B
// 	return nil
// }
