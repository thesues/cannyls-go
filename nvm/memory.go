package nvm

import (
	"errors"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"io"
)

type MemoryNVM struct {
	vec      []byte
	position uint64
}

func New(size uint64) (*MemoryNVM, error) {
	if !blockSize().IsAligned(size) {
		return nil, internalerror.InvalidInput
	}
	return &MemoryNVM{vec: make([]byte, size), position: 0}, nil
}

func NewFromVec(vec []byte) (*MemoryNVM, error) {
	if !blockSize().IsAligned(uint64(len(vec))) {
		return nil, internalerror.InvalidInput
	}

	return &MemoryNVM{vec: vec, position: 0}, nil
}

func (memory *MemoryNVM) Sync() error {
	return nil
}

func (memory *MemoryNVM) Position() uint64 {
	return memory.position
}

func (memory *MemoryNVM) Capacity() uint64 {
	return uint64(len(memory.vec))
}

func (memory *MemoryNVM) Split(p uint64) (sp1 *MemoryNVM, sp2 *MemoryNVM, err error) {
	if blockSize().CeilAlign(p) != p {
		return nil, nil, internalerror.InvalidInput
	}

	l, err := NewFromVec(memory.vec[0:p])
	if err != nil {
		return nil, nil, internalerror.InvalidInput
	}
	r, err := NewFromVec(memory.vec[p:])
	if err != nil {
		return nil, nil, internalerror.InvalidInput
	}

	return l, r, nil
}

func (memory *MemoryNVM) Seek(offset int64, whence int) (int64, error) {

	if !blockSize().IsAligned(uint64(offset)) {
		return -1, internalerror.InvalidInput
	}

	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = int64(memory.position) + offset
	case io.SeekEnd:
		abs = int64(len(memory.vec)) + offset
	default:
		return 0, errors.New("bytes.Reader.Seek: invalid whence")
	}

	if abs > int64(len(memory.vec)) || abs < 0 {
		return -1, internalerror.InvalidInput
	}

	memory.position = uint64(abs)

	return int64(abs), nil
}

func (memory *MemoryNVM) Read(buf []byte) (n int, err error) {
	if memory.position >= uint64(len(memory.vec)) {
		return 0, io.EOF
	}
	//TODO, maybe copyBuffer?
	n = copy(buf, memory.vec[memory.position:])
	memory.position += uint64(n)
	return n, nil
}

func (memory *MemoryNVM) Write(p []byte) (n int, err error) {
	if !blockSize().IsAligned(uint64(len(p))) {
		return -1, internalerror.InvalidInput
	}
	n = copy(memory.vec[memory.position:], p)
	memory.position += uint64(n)
	return n, nil
}

func blockSize() block.BlockSize {
	return block.Min()
}
