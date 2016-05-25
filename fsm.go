package canoe

import (
	"github.com/gorilla/mux"
)

type SnapshotData []byte

type FSM interface {
	// Be sparing with errors for all the following.
	// Err only if it results in bad state.
	// Because it will halt all the things
	Apply(entry LogData) error
	Snapshot() (SnapshotData, error)
	Restore(snap SnapshotData) error

	// Add handlefuncs to the mux router to have them appear at endpoint
	// 0.0.0.0:BIND_PORT/api/[endpoints]
	RegisterAPI(router *mux.Router)
}
