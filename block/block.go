package block

import (
	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/internalerror"
)

const (
	MIN uint16 = 512
)

type BlockSize uint16

func Min() BlockSize {
	bs, _ := NewBlockSize(MIN)
	return bs
}

func NewBlockSize(bs uint16) (BlockSize, error) {
	if bs < MIN {
		return 0, errors.Wrap(internalerror.InvalidInput, "blocksize is too small")
	}

	if bs%MIN != 0 {
		//return 0, internalerror.InvalidInput
		return 0, errors.Wrapf(internalerror.InvalidInput, "mod of %d is not 512", bs)
	}

	return BlockSize(bs), nil

}

func (bs BlockSize) CeilAlign(position uint64) uint64 {
	block_size := uint64(bs)
	return (position + block_size - 1) / block_size * block_size
}

func (bs BlockSize) FloorAlign(postion uint64) uint64 {
	block_size := uint64(bs)
	return postion / block_size * block_size
}

func (bs BlockSize) IsAligned(position uint64) bool {
	return position%uint64(bs) == 0

}

func (bs BlockSize) AsU16() uint16 {
	return uint16(bs)
}

func (bs BlockSize) AsU32() uint32 {
	return uint32(bs)
}

func (bs BlockSize) Contains(other BlockSize) bool {
	return uint64(bs) >= uint64(other) && uint64(bs)%uint64(other) == 0
}
