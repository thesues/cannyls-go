package portion

import (
	"fmt"
	"github.com/google/btree"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/block"
	_ "github.com/thesues/cannyls-go/internalerror"
)

type FreePortion uint64

/*
64bit
24    +    40
len        Address
FreePortion itself could support upto 24bit len
*/

//panic
func New(offset address.Address, size uint32) FreePortion {
	if size > (1<<24)-1 {
		panic("Address for FreePortion is too big")
	}
	n := uint64(size)<<40 + offset.AsU64()
	return FreePortion(n)
}

func FromDataPortion(dataPortion DataPortion) FreePortion {
	return New(dataPortion.start, uint32(dataPortion.len))
}

func DefaultFreePortion() FreePortion {
	return FreePortion(0)
}

func DefaultDataPortion() DataPortion {
	return DataPortion{
		start: address.AddressFromU32(0),
		len:   0,
	}
}

func DefaultJournalPortion() JournalPortion {
	return JournalPortion{
		start: address.AddressFromU32(0),
		len:   0,
	}
}

func (p FreePortion) Start() address.Address {
	n := uint64(p) & address.MAX_ADDRESS
	return address.AddressFromU64(n)
}

func (p FreePortion) End() address.Address {
	n := p.Start().AsU64() + (uint64(p) >> 40)
	return address.AddressFromU64(n)
}

func (p FreePortion) Len() uint32 {
	return uint32(uint64(p) >> 40)
}

func (p FreePortion) CheckedExtend(size uint32) (FreePortion, bool) {
	//bigger than 24bit
	if p.Len()+size > 0xFFFFFF {
		return DefaultFreePortion(), false
	}
	return New(p.Start(), p.Len()+size), true
}

//panic
func (p FreePortion) SlicePart(size uint16) (FreePortion, DataPortion) {
	if uint32(size) > p.Len() {
		panic("can not alloca dataportion from freeportionn")
	}
	alloc := DataPortion{
		start: p.Start(),
		len:   size,
	}

	new_start := p.Start().AsU64() + uint64(size)
	new_len := p.Len() - uint32(size)
	newFreePortion := New(address.AddressFromU64(new_start), new_len)
	return newFreePortion, alloc
}

type SizeBasedPortion FreePortion
type EndBasedPortion FreePortion

func (p SizeBasedPortion) Less(than btree.Item) bool {
	left := FreePortion(p)
	right := FreePortion(than.(SizeBasedPortion))
	if left.Len() < right.Len() {
		return true
	} else if left.Len() == right.Len() && left.Start().AsU64() < right.Start().AsU64() {
		return true
	}
	return false
}

func (p EndBasedPortion) Less(than btree.Item) bool {
	left := FreePortion(p)
	right := FreePortion(than.(EndBasedPortion))
	return left.End() < right.End()

}

type DataPortion struct {
	start address.Address
	len   uint16
}

func (p DataPortion) Display() string {
	return fmt.Sprintf("DataPortion: Start: %d, len :%d", p.start, p.len)
}

func (p DataPortion) ShiftBlockToBytes(b block.BlockSize) (offset uint64, size uint32) {
	s := b.AsU16()
	offset = p.start.AsU64() * uint64(s)
	size = uint32(p.len * s)
	return
}

func (p DataPortion) AsInts() (offset uint64, size uint16) {
	return p.start.AsU64(), p.len
}

func NewDataPortion(start uint64, size uint16) DataPortion {
	return DataPortion{
		start: address.AddressFromU64(start),
		len:   size,
	}
}

func (dp DataPortion) Len(b block.BlockSize) uint32 {
	return uint32(dp.len) * uint32(b.AsU16())
}

type Portion interface {
	Len(block.BlockSize) uint32
}

type JournalPortion struct {
	start address.Address
	len   uint16
}

func NewJournalPortion(start uint64, size uint16) JournalPortion {
	return JournalPortion{
		start: address.AddressFromU64(start),
		len:   size,
	}
}

func (jp JournalPortion) Len(b block.BlockSize) uint32 {
	return uint32(jp.len)
}

func (jp JournalPortion) Start() uint64 {
	return jp.start.AsU64()
}
