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

}
