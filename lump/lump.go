package lump

import (
	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/util/uint128"
)

type LumpId struct {
	uint128.Uint128
}

func FromU64(hi uint64, lo uint64) LumpId {
	return LumpId{uint128.FromInts(hi, lo)}
}
func FromBytes(vec []byte) (LumpId, error) {
	if len(vec) > 16 {
		return EmptyLump(), errors.Wrap(internalerror.InvalidInput, "from bytes to lumpId failed")
	}
	return LumpId{uint128.FromBytes(vec)}, nil
}

func FromString(s string) (LumpId, error) {
	n, err := uint128.FromString(s)
	if err != nil {
		return EmptyLump(), errors.Wrap(err, "from string to lumpId failed")
	}
	return LumpId{n}, nil
}

func (left LumpId) Compare(right LumpId) int {
	return left.Uint128.Compare(right.Uint128)
}

func EmptyLump() LumpId {
	return LumpId{uint128.FromInts(0, 0)}
}

//lump data
const (
	LUMP_MAX_SIZE     = 0xFFFF * (512 - 2)
	MAX_EMBEDDED_SIZE = 0xFFFF
)

type LumpDataInner int

const (
	JournalRegion = iota
	DataRegion

//	DataRegionUnaligned
)

type LumpData struct {
	Inner *block.AlignedBytes
}

type LumpEmbededData struct {
	Inner []byte
}

/*
func NewLumpDataUnaligned(buf []byte) (*LumpData, error) {
	if len(buf) > LUMP_MAX_SIZE {
		return nil, errors.Wrapf(internalerror.InvalidInput, "lump data is too big %d", len(buf))
	}
	return &LumpData{
		inner:     buf,
		innerType: DataRegionUnaligned,
	}, nil
}
*/

/*
func NewLumpDataEmbedded(buf []byte) (*LumpData, error) {
	if len(buf) > MAX_EMBEDDED_SIZE {
		return nil, errors.Wrapf(internalerror.InvalidInput, "lump data is too big %d", len(buf))
	}
	return &LumpData{
		inner:     buf,
		innerType: JournalRegion,
	}, nil
}
*/

//TODO, to be aligned at upper
func NewLumpDataAligned(size int, blockSize block.BlockSize) LumpData {
	ab := block.NewAlignedBytes(size, blockSize)
	return LumpData{
		Inner: ab,
	}
}

func NewLumpDataWithAb(ab *block.AlignedBytes) LumpData {
	return LumpData{
		Inner: ab,
	}
}

func (l LumpData) AsBytes() []byte {
	return l.Inner.AsBytes()
}
