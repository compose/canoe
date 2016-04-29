package ha

import (
	"encoding/binary"
	"github.com/satori/go.uuid"
)

func Uint64UUID() uint64 {
	return binary.LittleEndian.Uint64(uuid.NewV4().Bytes())
}
