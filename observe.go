package raftwrapper

import (
	"sync/atomic"
)

type Observation interface{}

type FilterFn func(o Observation) bool

var nextObserverId uint64

type Observer struct {
	channel chan Observation
	filter  FilterFn
	id      uint64
}

func NewObserver(channel chan Observation, filter FilterFn) *Observer {
	return &Observer{
		channel: channel,
		filter:  filter,
		id:      atomic.AddUint64(&nextObserverId, 1),
	}
}

func (rn *Node) observe(data Observation) {
	rn.observersLock.RLock()
	defer rn.observersLock.RUnlock()
	for _, observer := range rn.observers {
		if observer.filter != nil && !observer.filter(interface{}(&data).(*Observation)) {
			continue
		}
		if observer.channel == nil {
			continue
		}

		// make sure we don't block if consumer isn't consuming fast enough
		select {
		case observer.channel <- data:
			continue
		default:
			continue
		}
	}
}

func (rn *Node) RegisterObserver(o *Observer) {
	rn.observersLock.Lock()
	defer rn.observersLock.Unlock()
	rn.observers[o.id] = o
}

func (rn *Node) UnregisterObserver(o *Observer) {
	rn.observersLock.Lock()
	defer rn.observersLock.Unlock()
	delete(rn.observers, o.id)
}
