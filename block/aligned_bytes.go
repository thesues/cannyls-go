package block

import (
	"fmt"
	"unsafe"
)

var _ = fmt.Printf

type AlignedBytes struct {
	buf    []byte
	len    uint32
	block  BlockSize
	offset uint16
}

//https://github.com/ncw/directio/blob/master/direct_io.go
func alignment(block []byte, AlignSize uint16) int {
	//address % 512
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(AlignSize-1))
}

func createNewAlignedBuf(size int, blockSize BlockSize) (uint16, []byte) {

	// + 512 for aligning the offset
	// -1 for floor_aligin in the function Capactiy
	capacity := blockSize.CeilAlign(uint64(size)) + uint64(blockSize.AsU16()) - 1

	buf := make([]byte, capacity, capacity)
	a := alignment(buf, blockSize.AsU16())
	var offset uint16
	if a != 0 {
		offset = blockSize.AsU16() - uint16(a)
	}

	return offset, buf

}

func NewAlignedBytes(size int, blockSize BlockSize) *AlignedBytes {

	offset, buf := createNewAlignedBuf(size, blockSize)

	return &AlignedBytes{
		buf:    buf,
		len:    uint32(size),
		block:  blockSize,
		offset: offset,
	}
}

func (ab *AlignedBytes) BlockSize() BlockSize {
	return ab.block
}

//FromBytes may use the origin buffer,so Do not use this buf again
func FromBytes(src []byte, blockSize BlockSize) *AlignedBytes {
	ab, _ := fromBytes(src, blockSize)
	return ab
}

//for debug
func fromBytes(src []byte, blockSize BlockSize) (*AlignedBytes, bool) {
	a := alignment(src, blockSize.AsU16())
	capacity := blockSize.CeilAlign(uint64(len(src)))
	if a != 0 || capacity > uint64(cap(src)) {
		newAlignedBytes := NewAlignedBytes(len(src), blockSize)
		copy(newAlignedBytes.buf, src)
		return newAlignedBytes, true
	}
	return &AlignedBytes{
		buf:    src[:cap(src)],
		len:    uint32(len(src)),
		offset: 0,
		block:  blockSize,
	}, false
}

func (ab *AlignedBytes) Align() *AlignedBytes {
	ab.len = uint32(ab.block.CeilAlign(uint64(ab.len)))
	return ab
}

func (ab *AlignedBytes) AsBytes() []byte {
	return ab.buf[ab.offset : uint32(ab.offset)+ab.len]
}

func (ab *AlignedBytes) Resize(newLen uint32) {
	newCapacity := ab.block.CeilAlign(uint64(newLen))
	if int(newCapacity) > len(ab.buf)-int(ab.offset) {
		offset, newBuf := createNewAlignedBuf(int(newCapacity), ab.block)

		//only the block size is the same as before
		copy(newBuf[offset:], ab.AsBytes())
		ab.buf = newBuf
		ab.offset = offset
	}
	ab.len = newLen
}

func (ab *AlignedBytes) AlignResize(newLen uint32) {
	ab.Resize(newLen)
	ab.Align()
}

func (ab *AlignedBytes) Len() uint32 {
	return ab.len
}

func (ab *AlignedBytes) Truncate(len uint32) {
	if len < ab.len {
		ab.len = len
	}
}

func (ab *AlignedBytes) Capacity() uint64 {
	return ab.block.FloorAlign(uint64(len(ab.buf)))
}
