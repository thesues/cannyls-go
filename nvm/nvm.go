package nvm

import (
	"errors"
	"io"

	"github.com/thesues/cannyls-go/block"
)

type NonVolatileMemory interface {
	io.ReadWriteSeeker
	io.Closer
	io.ReaderAt
	Sync() error
	Position() uint64
	Capacity() uint64
	BlockSize() block.BlockSize
	Split(position uint64) (NonVolatileMemory, NonVolatileMemory, error)
	RawSize() int64
}

var (
	MAGIC_NUMBER = [4]byte{'l', 'u', 's', 'f'}
)

const (
	MAJOR_VERSION           uint16 = 2
	MINOR_VERSION           uint16 = 1
	MAX_JOURNAL_REGION_SIZE uint64 = (1 << 40) - 1
	MAX_DATA_REGION_SIZE    uint64 = MAX_JOURNAL_REGION_SIZE * uint64(block.MIN)
)

func ConvertToOffset(nvm NonVolatileMemory, offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = int64(nvm.Position()) + offset
	case io.SeekEnd:
		abs = int64(nvm.Capacity()) + offset
	default:
		return 0, errors.New("bytes.Reader.Seek: invalid whence")
	}
	return abs, nil
}
