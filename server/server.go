package server

import (
	"errors"
	"sync"

	"../shared"
)

type Server struct {
	ID      string
	Objects map[string]Object
}

func NewServer(id string) *Server {
	server := new(Server)
	server.ID = id
	server.Objects = make(map[string]Object)
	return server
}

type Object struct {
	Value        string
	Readers      *shared.StringSet // Transaction ID
	Writer       string            // Transaction ID
	RequestQueue []*LockRequest
	m            sync.Mutex
}

func NewObject(value string) *Object {
	object := new(Object)
	object.Value = value
	object.Readers = shared.NewSet()
	object.Writer = ""
	object.RequestQueue = make([]*LockRequest, 0)
	return object
}

type LockRequest struct {
	Type          string // read, write, upgrade
	TransactionID string
	Channel       chan bool // granted, abort
}

func NewLockRequest(t string, tid string) *LockRequest {
	req := new(LockRequest)
	req.Type = t
	req.TransactionID = tid
	req.Channel = make(chan bool)
	return req
}

type Args struct {
	Key           string
	Value         string // Value ignored in GET commands
	TransactionID string
}

// ReleaseLock : release lock on object for transaction
func (server *Server) ReleaseLock(args *Args, reply *string) error {
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

// func (server *Server) ReleaseAllLocks(args *Args, reply *string) error {

// 	return nil
// }

// WriterLock : Get exclusive lock to object
func (server *Server) WriterLock(args *Args, reply *string) error {
	obj, found := server.Objects[args.Key]

	// New object immediately gets writer access
	if !found {
		*reply = "SUCCESS"
		return nil
	}

	obj.m.Lock()
	if obj.Writer == "" {
		if obj.Readers.Size() == 0 {
			// No reader/writer, immediately grant
			obj.Writer = args.TransactionID
			*reply = "SUCCESS"
			obj.m.Unlock()
			return nil
		} else {
			// No writer, has readers
			if obj.Readers.SetHas(args.TransactionID) {
				// Transaction is already reading
				if obj.Readers.Size() == 1 {
					// Immediately Promote
					obj.Writer = args.TransactionID
					obj.Readers.SetDelete(args.TransactionID)
					*reply = "SUCCESS"
					obj.m.Unlock()
					return nil
				} else {
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
					return nil
				}
			} else {
				// Transaction is not reader, wait for writer lock
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
				return nil
			}
		}
	} else {
		// Transaction is not reader, wait for writer lock
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
		return nil
	}

	obj.m.Unlock()

	return nil
}

// func (server *Server) TryGet(args *Args, reply *string) error {

// 	return nil
// }

func (server *Server) ReaderLock(args *Args, reply *string) error {
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
func (server *Server) Set(args *Args, reply *string) error {

	return nil
}

func (server *Server) Commit(args *Args, reply *string) error {

	return nil
}

func (server *Server) Abort(args *Args, reply *string) error {

	return nil
}

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
