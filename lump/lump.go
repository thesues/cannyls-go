package lump

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	"strconv"

	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
)

type LumpId struct {
	lo uint64
}

func FromU64(hi uint64, lo uint64) LumpId {
	return LumpId{lo: lo}
}
func FromBytes(vec []byte) (LumpId, error) {
	if len(vec) != 8 {
		return LumpId{}, errors.Wrap(internalerror.InvalidInput, "from bytes to lumpId failed")
	}
	n := binary.BigEndian.Uint64(vec[:8])
	return LumpId{lo: n}, nil
}

func (id LumpId) Inc() LumpId {
	return LumpId{
		lo: id.lo + 1,
	}
}

func (id LumpId) IsMax() bool {
	return id.lo == math.MaxUint64
}

func FromString(s string) (LumpId, error) {
	n, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return LumpId{}, errors.Wrap(internalerror.InvalidInput, "from string to lumpId failed")
	}
	return LumpId{lo: n}, nil
}

func (id LumpId) String() string {
	return strconv.FormatUint(id.lo, 16)
}

func (id LumpId) U64() uint64 {
	return id.lo
}

func (id LumpId) GetBytes() []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id.lo)
	return b[:]
}

func (id LumpId) Write(w io.Writer) (int, error) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id.lo)
	return w.Write(b[:])
}

func (left LumpId) Compare(right LumpId) int {
	return int(left.lo - right.lo)
}

func EmptyLump() LumpId {
	return LumpId{}
}

//lump data
const (
	LUMP_MAX_SIZE     = 0xFFFF*(512) - 2
	MAX_EMBEDDED_SIZE = 0xFFFF
)

type LumpDataInner int

const (
	JournalRegion = iota
	DataRegion

//	DataRegionUnaligned
)

type LumpData struct {
	Inner      *block.AlignedBytes
	originSize uint32
}

//TODO, to be aligned at upper
func NewLumpDataAligned(size int, blockSize block.BlockSize) LumpData {

	if size > LUMP_MAX_SIZE {
		return LumpData{
			Inner: nil,
		}
	}
	ab := block.NewAlignedBytes(size, blockSize)

	return LumpData{
		Inner:      ab,
		originSize: 0,
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

//helper functions
var abDataSizeClass [16]uint32
var abDataSizePools [16]sync.Pool

func init() {
	abDataSizeClass[0] = (1 << 10)
	for i := 1; i < 16; i++ {
		size := abDataSizeClass[i-1] * 2
		if size > LUMP_MAX_SIZE {
			size = LUMP_MAX_SIZE
		}
		abDataSizeClass[i] = size
	}

	for i := 0; i < 16; i++ {
		size := abDataSizeClass[i]
		abDataSizePools[i] = sync.Pool{
			New: func() interface{} {
				return NewLumpDataAligned(int(size), block.Min())
			},
		}
	}

}

func GetLumpData(size int) LumpData {
	//limit
	if size > (512 << 10) {
		return NewLumpDataAligned(size, block.Min())
	}
	//if size < 512K, allocate memory from sync.Pool

	i := 0
	for ; i < len(abDataSizeClass); i++ {
		if uint32(size) <= abDataSizeClass[i] {
			break
		}
	}
	ab := abDataSizePools[i].Get().(LumpData)
	ab.Inner.Resize(uint32(size))
	ab.originSize = abDataSizeClass[i]
	return ab
}

func PutLumpData(data LumpData) error {
	//limit
	if data.originSize == 0 {
		return nil
	}
	for i, n := range abDataSizeClass {
		if data.originSize == n {
			if data.Inner.Resize(data.originSize) {
				panic(fmt.Sprintf("returned buffer is not big enough %d\n", data.originSize))
			}
			abDataSizePools[i].Put(data)
			return nil
		}
	}
	panic(fmt.Sprintf("did not find a size for originSize"))
}
