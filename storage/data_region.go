package storage

import (
	_ "fmt"
	"github.com/thesues/cannyls-go/block"
	_ "github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/portion"
	"github.com/thesues/cannyls-go/storage/allocator"
	"io"
)

const (
	LUMP_DATA_TRAILER_SIZE = 2
)

type DataRegion struct {
	allocator  *allocator.DataPortionAllocator
	nvm        nvm.NonVolatileMemory
	block_size block.BlockSize
}

func NewDataRegion(alloc *allocator.DataPortionAllocator, nvm nvm.NonVolatileMemory) *DataRegion {
	return &DataRegion{
		allocator:  alloc,
		nvm:        nvm,
		block_size: block.Min(),
	}
}

func (region *DataRegion) shiftBlockSize(size uint32) uint32 {
	local_size := uint32(region.block_size.AsU16())
	return (size + uint32(local_size) - 1) / local_size

}

/*
* data region format on disk
*        0                   1                   2                   3
       0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         Lump Data (Variable)
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         Padding (Variable)
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |         Padding size          |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/

//WARNING: this PUT would CHANGE (data *lump.LumpData),
func (region *DataRegion) Put(data *lump.LumpData) (portion.DataPortion, error) {
	//
	size := data.Inner.Len() + LUMP_DATA_TRAILER_SIZE

	//Aligned
	data.Inner.AlignResize(size)

	trailer_offset := data.Inner.Len() - LUMP_DATA_TRAILER_SIZE
	padding_len := data.Inner.Len() - size

	if padding_len >= uint32(data.Inner.BlockSize().AsU16()) {
		panic("data region put's align is wrong")
	}
	putUint16BigEndian(data.Inner.AsBytes()[trailer_offset:], uint16(padding_len))

	required_blocks := region.shiftBlockSize(data.Inner.Len())
	data_portion, err := region.allocator.Allocate(uint16(required_blocks))
	if err != nil {
		return portion.DefaultDataPortion(), err
	}

	offset, len := data_portion.FromBlockToBytes(region.block_size)
	if len != data.Inner.Len() {
		panic("should be the same in data_region put")
	}
	if _, err = region.nvm.Seek(int64(offset), io.SeekStart); err != nil {
		return data_portion, err
	}
	if _, err = region.nvm.Write(data.Inner.AsBytes()); err != nil {
		return data_portion, err
	}

	return data_portion, err
}

func (region *DataRegion) Release(portion portion.DataPortion) {
	region.allocator.Release(portion)
}

func (region *DataRegion) Get(portion portion.DataPortion) (*lump.LumpData, error) {
	offset, len := portion.FromBlockToBytes(region.block_size)

	if _, err := region.nvm.Seek(int64(offset), io.SeekStart); err != nil {
		return lump.DefaultLumpData(), err
	}

	ab := block.NewAlignedBytes(int(len), region.block_size)

	if _, err := region.nvm.Read(ab.AsBytes()); err != nil {
		return lump.DefaultLumpData(), err
	}

	padding_size := uint32(getUint16BigEndion(ab.AsBytes()[ab.Len()-2:]))

	ab.Resize(ab.Len() - padding_size - LUMP_DATA_TRAILER_SIZE)
	return lump.NewLumpDataWithAb(ab), nil
}

//binary helper functions
func putUint16BigEndian(buf []byte, n uint16) {
	if len(buf) != 2 {
		panic("in putUint16BigEndian")
	}
	hi := (n & 0xFF00) >> 8
	lo := (n & 0x00FF)
	buf[0] = byte(hi)
	buf[1] = byte(lo)
}

func getUint16BigEndion(buf []byte) (n uint16) {
	if len(buf) != 2 {
		panic("in getUint16BigEndian")
	}
	var hi = uint16(buf[0]) << 8
	var lo = uint16(buf[1])
	return hi | lo
}
