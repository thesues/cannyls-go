package block

import ()

type AlignedBytes struct {
	buf   []byte
	len   uint32
	block BlockSize
}

func New(size int, blockSize BlockSize) *AlignedBytes {
	capacity := blockSize.CeilAlign(uint64(size))
	buf := make([]byte, capacity, capacity)
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
	newAlignedBytes := New(len(src), blockSize)
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
		newBuf := make([]byte, newCapacity, newCapacity)
		copy(newBuf, ab.buf)
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
