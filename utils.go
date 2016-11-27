package canoe

import (
	"encoding/binary"
	"github.com/satori/go.uuid"
	"sync/atomic"
)

// Uint64UUID returns a UUID encoded to uint64
func Uint64UUID() uint64 {
	return binary.LittleEndian.Uint64(uuid.NewV4().Bytes())
}

type AtomicBool int32

func (a *AtomicBool) Set() {
	atomic.StoreInt32((*int32)(a), 1)
}

func (a *AtomicBool) Unset() {
	atomic.StoreInt32((*int32)(a), 0)
}

func (a *AtomicBool) IsSet() bool {
	return atomic.LoadInt32((*int32)(a)) == 1
}
