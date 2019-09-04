package storage

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/portion"
	"github.com/thesues/cannyls-go/storage/allocator"
	"github.com/thesues/cannyls-go/util"
)

type DataRegion struct {
	allocator  allocator.DataPortionAlloc
	nvm        nvm.NonVolatileMemory
	block_size block.BlockSize
}

func NewDataRegion(alloc allocator.DataPortionAlloc, nvm nvm.NonVolatileMemory) *DataRegion {
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
func (region *DataRegion) Put(data lump.LumpData) (portion.DataPortion, error) {
	//
	size := data.Inner.Len() + portion.LUMP_DATA_TRAILER_SIZE

	//Aligned
	data.Inner.AlignResize(size)

	trailer_offset := data.Inner.Len() - portion.LUMP_DATA_TRAILER_SIZE
	padding_len := data.Inner.Len() - size

	if padding_len >= uint32(data.Inner.BlockSize().AsU16()) {
		panic("data region put's align is wrong")
	}
	util.PutUINT16(data.Inner.AsBytes()[trailer_offset:], uint16(padding_len))

	required_blocks := region.shiftBlockSize(data.Inner.Len())
	data_portion, err := region.allocator.Allocate(uint16(required_blocks))

	if err != nil {
		return portion.DataPortion{}, err
	}

	offset, len := data_portion.ShiftBlockToBytes(region.block_size)
	if len != data.Inner.Len() {
		panic(fmt.Sprintf("should be the same in data_region put userdata:%d , diskdata:%d",
			data.Inner.Len(), len))
		//FIXME
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

func (region *DataRegion) GetSize(dataPortion portion.DataPortion) (size uint32, err error) {
	_, err = region.nvm.Seek(int64(dataPortion.ShiftToPaddingBlock(region.block_size)),
		io.SeekStart)
	if err != nil {
		return 0, err
	}
	buf := make([]byte, region.block_size)
	n, err := region.nvm.Read(buf)
	if err != nil {
		return 0, err
	}
	if n != int(region.block_size) {
		return 0, errors.New("not enough bytes read, early EOF")
	}
	paddingSize := uint32(util.GetUINT16(buf[region.block_size-2:]))
	size = uint32(dataPortion.Len)*uint32(region.block_size) -
		paddingSize - portion.LUMP_DATA_TRAILER_SIZE
	return size, nil
}

func (region *DataRegion) Get(dataPortion portion.DataPortion) (lump.LumpData, error) {
	offset, len := dataPortion.ShiftBlockToBytes(region.block_size)

	if _, err := region.nvm.Seek(int64(offset), io.SeekStart); err != nil {
		return lump.LumpData{}, err
	}

	ab := block.NewAlignedBytes(int(len), region.block_size)

	if _, err := region.nvm.Read(ab.AsBytes()); err != nil {
		return lump.LumpData{}, err
	}

	padding_size := uint32(util.GetUINT16(ab.AsBytes()[ab.Len()-2:]))

	ab.Resize(ab.Len() - padding_size - portion.LUMP_DATA_TRAILER_SIZE)
	return lump.NewLumpDataWithAb(ab), nil
}

//more friendly data portion read. only read up user required data.
//the returned bytes could be less than length
func (region *DataRegion) GetWithOffset(dataPortion portion.DataPortion,
	startOffset uint32, length uint32) ([]byte, error) {

	offset, len := dataPortion.ShiftBlockToBytes(region.block_size)

	if startOffset+length > len-portion.LUMP_DATA_TRAILER_SIZE {
		return nil, errors.Wrap(internalerror.InvalidInput, "given length is too big")
	}

	newReadStart := region.block_size.FloorAlign(offset + uint64(startOffset))
	prefixPadding := startOffset % region.block_size.AsU32()

	ab := block.NewAlignedBytes(int(length+prefixPadding), region.block_size)
	ab.Align()

	if _, err := region.nvm.Seek(int64(newReadStart), io.SeekStart); err != nil {
		return nil, err
	}

	if _, err := region.nvm.Read(ab.AsBytes()); err != nil {
		return nil, err
	}

	//If length is small, and the read op does't reach the last block
	if length+prefixPadding <= len-region.block_size.AsU32() {
		return ab.AsBytes()[prefixPadding : prefixPadding+length], nil
	}

	//In this case, if length is too big(reach to the last block), prevent to read the padding data
	padding_size := uint32(util.GetUINT16(ab.AsBytes()[ab.Len()-2:]))
	realFileSize := util.Min32(ab.Len()-padding_size-portion.LUMP_DATA_TRAILER_SIZE, prefixPadding+length)
	return ab.AsBytes()[prefixPadding:realFileSize], nil

}
