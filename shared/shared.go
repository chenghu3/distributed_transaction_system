package shared

import (
	"math/rand"
	"net"
	"sync"
)

// ************************************* //
// *****  StringSet defination ********* //
// ************************************* //

// StringSet : Customized thread-safe set data structure for String
type StringSet struct {
	set    map[string]bool
	RWlock sync.RWMutex
}

// NewSet : Construntor for StringSet
func NewSet() *StringSet {
	s := new(StringSet)
	s.set = make(map[string]bool)
	return s
}

// SetAdd : Add method for StringSet
func (set *StringSet) SetAdd(s string) bool {
	set.RWlock.Lock()
	_, found := set.set[s]
	set.set[s] = true
	set.RWlock.Unlock()
	return !found //False if it existed already
}

// SetDelete : Delete method for StringSet
func (set *StringSet) SetDelete(s string) bool {
	set.RWlock.Lock()
	defer set.RWlock.Unlock()
	_, found := set.set[s]
	if !found {
		return false // not such element
	}
	delete(set.set, s)
	return true
}

// SetHas : Check whether String is in StringSet
func (set *StringSet) SetHas(s string) bool {
	set.RWlock.RLock()
	_, found := set.set[s]
	set.RWlock.RUnlock()
	return found
}

// SetToArray : Set to array
func (set *StringSet) SetToArray() []string {
	set.RWlock.RLock()
	defer set.RWlock.RUnlock()
	keys := make([]string, 0)
	for k := range set.set {
		keys = append(keys, k)
	}
	return keys
}

// Size : size
func (set *StringSet) Size() (size int) {
	set.RWlock.RLock()
	defer set.RWlock.RUnlock()
	size = len(set.set)
	return
}

// GetRandom : Get a random element from set
func (set *StringSet) GetRandom() string {
	set.RWlock.RLock()
	defer set.RWlock.RUnlock()
	if len(set.set) == 0 {
		return ""
	}
	i := rand.Intn(len(set.set))
	for k := range set.set {
		if i == 0 {
			return k
		}
		i--
	}
	panic("never!!")
}

// ************************************* //
// *****  CommandQueue defination ****** //
// ************************************* //

// CommandQueue is queue of client command (thread-safe)
type CommandQueue struct {
	Commands    []string
	NotifyChann chan bool
	lock        sync.RWMutex
}

// NewQueue creates a new CommandQueue
func NewQueue() *CommandQueue {
	q := new(CommandQueue)
	q.Commands = make([]string, 0)
	q.NotifyChann = make(chan bool, 1)
	return q
}

// Push adds an commannd to the end of the queue
func (q *CommandQueue) Push(c string) {
	q.lock.Lock()
	if len(q.Commands) == 0 {
		q.NotifyChann <- true
	}
	q.Commands = append(q.Commands, c)
	q.lock.Unlock()
}

// Pop removes an Item from the start of the queue
func (q *CommandQueue) Pop() string {
	q.lock.Lock()
	res := q.Commands[0]
	q.Commands = q.Commands[1:len(q.Commands)]
	q.lock.Unlock()
	return res
}

// IsEmpty returns true if the queue is empty
func (q *CommandQueue) IsEmpty() bool {
	q.lock.RLock()
	defer q.lock.RUnlock()
	return len(q.Commands) == 0
}

// Clear empty the queue
func (q *CommandQueue) Clear() {
	q.lock.Lock()
	q.Commands = make([]string, 0)
	q.lock.Unlock()
}

// Args : argument for rpc calls
type Args struct {
	Key           string
	Value         string // Value ignored in GET commands
	TransactionID string
}

type CoordinatorArgs struct {
	From string
	To   []string
}

// GetLocalIP returns the non loopback local IP of the host
// Reference https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
