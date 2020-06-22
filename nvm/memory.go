package nvm

import (
	"io"

	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
)

type MemoryNVM struct {
	vec      []byte
	position uint64
}

func New(size uint64) (*MemoryNVM, error) {
	if !block.Min().IsAligned(size) {
		return nil, internalerror.InvalidInput
	}
	return &MemoryNVM{vec: make([]byte, size), position: 0}, nil
}

func NewFromVec(vec []byte) (*MemoryNVM, error) {
	if !block.Min().IsAligned(uint64(len(vec))) {
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

func (memory *MemoryNVM) RawSize() int64 {
	return -1
}

func (memory *MemoryNVM) Split(p uint64) (sp1 NonVolatileMemory, sp2 NonVolatileMemory, err error) {
	if memory.BlockSize().CeilAlign(p) != p {
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

	if !memory.BlockSize().IsAligned(uint64(offset)) {
		return -1, errors.Wrapf(internalerror.InvalidInput, "block size is not aligned, offset: %d", offset)
	}

	abs, err := ConvertToOffset(memory, offset, whence)
	if err != nil {
		return 0, err
	}
	if abs > int64(len(memory.vec)) || abs < 0 {
		return -1, errors.Wrapf(internalerror.InvalidInput, "seek size is too big abs: %d", abs)
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
	if !memory.BlockSize().IsAligned(uint64(len(p))) {
		return -1, internalerror.InvalidInput
	}
	n = copy(memory.vec[memory.position:], p)
	memory.position += uint64(n)
	return n, nil
}

func (memory *MemoryNVM) Close() error {
	return nil
}

func (memory *MemoryNVM) BlockSize() block.BlockSize {
	return block.Min()
}

//Thread-Safe
func (memory *MemoryNVM) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(memory.vec)) {
		return 0, io.EOF
	}
	n = copy(p, memory.vec[memory.position:])
	return n, nil
}

//for local test
func (memory *MemoryNVM) AsBytes() []byte {
	return memory.vec
}
