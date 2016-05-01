package raftwrapper

import (
	"sync/atomic"
)

type Observation interface{}

type FilterFn func(o *Opservation) bool

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
		id:      atomic.AddUnit64(&nextObserverId, 1),
	}
}

func (rn *raftNode) observe(data LogData) {
	r.observersLock.RLock()
	defer r.observersLock.RUnlock()
	for _, observer := range r.observers {
		if observer.filter != nil && !observer.filter(&data) {
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

func (rn *raftNode) RegisterObserver(o *Observer) {
	r.observersLock.Lock()
	defer r.observersLock.Unlock()
	r.observers[o.id] = o
}

func (rn *raftNode) UnregisterObserver(o *Observer) {
	r.observersLock.Lock()
	defer r.observersLock.Unlock()
	delete(r.observers, o.id)
}
