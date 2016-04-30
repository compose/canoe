package raftwrapper

import (
	"fmt"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/gorilla/mux"
)

type FSM interface {
	Apply(entry LogData)
	Snapshot() (raftpb.Snapshot, error)
	Restore(snap raftpb.Snapshot) error
	RegisterAPI(router *mux.Router)
}
