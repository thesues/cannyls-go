package block

import (
	"unsafe"
)

type AlignedBytes struct {
	buf   []byte
	len   uint32
	block BlockSize
}

//https://github.com/ncw/directio/blob/master/direct_io.go
func alignment(block []byte, AlignSize uint16) int {
	//address % 512
	return int(uintptr(unsafe.Pointer(&block[0])) & uintptr(AlignSize-1))
}

func createNewAlignedBuf(size int, blockSize BlockSize) []byte {

	// + 512 for aligning the offset
	// -1 for floor_aligin in the function Capactiy
	capacity := blockSize.CeilAlign(uint64(size)) + uint64(blockSize.AsU16()) - 1

	buf := make([]byte, capacity, capacity)
	a := alignment(buf, blockSize.AsU16())
	var offset uint16
	if a != 0 {
		offset = blockSize.AsU16() - uint16(a)
	}

	buf = buf[offset:]
	return buf

}

func NewAlignedBytes(size int, blockSize BlockSize) *AlignedBytes {

	buf := createNewAlignedBuf(size, blockSize)

	return &AlignedBytes{
		buf:   buf,
		len:   uint32(size),
		block: blockSize,
	}
}

func (ab *AlignedBytes) Capacity() uint64 {
	return ab.block.FloorAlign(uint64(len(ab.buf)))
}

func (ab *AlignedBytes) BlockSize() BlockSize {
	return ab.block
}

func FromBytes(src []byte, blockSize BlockSize) *AlignedBytes {
	newAlignedBytes := NewAlignedBytes(len(src), blockSize)
	copy(newAlignedBytes.buf, src)
	return newAlignedBytes
}

func (ab *AlignedBytes) Align() *AlignedBytes {
	ab.len = uint32(ab.block.CeilAlign(uint64(ab.len)))
	return ab
}

func (ab *AlignedBytes) AsBytes() []byte {
	return ab.buf[:ab.len]
}

func (ab *AlignedBytes) Resize(newLen uint32) {
	if newLen > uint32(len(ab.buf)) {
		newCapacity := ab.block.CeilAlign(uint64(newLen))

		newBuf := createNewAlignedBuf(int(newCapacity), ab.block)

		copy(newBuf, ab.buf)
		ab.buf = newBuf
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
