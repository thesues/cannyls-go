package allocator

import (
	"fmt"
	"sort"

	"math"

	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/portion"
	"github.com/thesues/cannyls-go/util"
	"github.com/thesues/go-judy"
)

//the Tree here means a set
type JudyPortionAlloc struct {
	startBasedTree judy.Judy1
	sizeBasedTree  judy.Judy1
}

type JudyPortion uint64

/*
JudyPortion format:
JudyPortion could be sorted by end_address, JudyPortion is stored in endBasedTree
64bit
40            +    24
start_address +    len
*/

/*SizeBased uint64 format:
SizedBased uint64 could be sorted by len, it is stored in sizedBaseTree
64bit
24    +    40
len   +    start_address
*/

func newJudyPortion(start address.Address, size uint32) JudyPortion {
	if size > (1<<24)-1 {
		panic("Address for FreePortion is too big")
	}
	startAddress := start.AsU64()
	n := (startAddress << 24) | uint64(size)
	return JudyPortion(n)
}

//Len is 24bit
const MAX_OFFSET = (1 << 24) - 1

func (judy JudyPortion) Len() uint32 {
	n := uint64(judy)
	return uint32(n & MAX_OFFSET)
}

func (judy JudyPortion) Start() address.Address {
	n := uint64(judy)
	return address.AddressFromU64(n >> 24)
}

func (judy JudyPortion) End() address.Address {
	n := uint64(judy)
	return address.AddressFromU64((n >> 24) + uint64(judy.Len()))
}

func (judy JudyPortion) CheckedExtend(size uint32) bool {
	//bigger than 24bit
	if judy.Len()+size > 0xFFFFFF {
		return false
	}
	return true
}

func (p JudyPortion) SlicePart(size uint16) (JudyPortion, portion.DataPortion) {
	if uint32(size) > p.Len() {
		panic("can not alloca dataportion from freeportionn")
	}
	allocated := portion.DataPortion{
		Start: p.Start(),
		Len:   size,
	}

	new_start := p.Start().AsU64() + uint64(size)
	new_len := p.Len() - uint32(size)
	newJudyPortion := newJudyPortion(address.AddressFromU64(new_start), new_len)
	return newJudyPortion, allocated
}

func fromDataPortionToJudy(p portion.DataPortion) JudyPortion {
	return newJudyPortion(p.Start, uint32(p.Len))
}

func fromSizebasedToJudy(n uint64) JudyPortion {
	startAddress := n & address.MAX_ADDRESS
	len := n >> 40
	return newJudyPortion(address.AddressFromU64(startAddress), uint32(len))
}

func (judy JudyPortion) ToSizeBasedUint64() uint64 {
	return uint64(judy.Len())<<40 | judy.Start().AsU64()
}

func NewJudyAlloc() *JudyPortionAlloc {
	alloc := &JudyPortionAlloc{
		startBasedTree: judy.Judy1{},
		sizeBasedTree:  judy.Judy1{},
	}
	return alloc
}

//capacitySector will always less than (1<<24),
func BuildJudyAlloc(capacitySector uint32) *JudyPortionAlloc {
	alloc := NewJudyAlloc()
	alloc.addPortion(newJudyPortion(address.AddressFromU64(0), capacitySector))
	return alloc
}

func (alloc *JudyPortionAlloc) MemoryUsed() uint64 {
	return alloc.startBasedTree.MemoryUsed() + alloc.sizeBasedTree.MemoryUsed()
}

//for debug
func (alloc *JudyPortionAlloc) allPortions() (sizeBased []JudyPortion, startBased []JudyPortion) {
	sizeBased = make([]JudyPortion, 0, alloc.sizeBasedTree.CountAll())
	startBased = make([]JudyPortion, 0, alloc.startBasedTree.CountAll())

	index, ok := alloc.sizeBasedTree.First(0)
	for ok {
		p := fromSizebasedToJudy(index)
		sizeBased = append(sizeBased, p)
		index, ok = alloc.sizeBasedTree.Next(index)
	}

	index, ok = alloc.startBasedTree.First(0)
	for ok {
		p := JudyPortion(index)
		startBased = append(startBased, p)
		index, ok = alloc.startBasedTree.Next(index)
	}
	return
}

func (alloc *JudyPortionAlloc) Display() {

	sizeBased, startBased := alloc.allPortions()
	fmt.Printf("==Size Based Tree==\n")
	for _, p := range sizeBased {
		fmt.Printf("Portion Size: %d, Start %d, End: %d\n", p.Len(), p.Start(), p.End())
	}

	fmt.Printf("==Start Based Tree==\n")

	for _, p := range startBased {
		fmt.Printf("Portion Size: %d, Start %d, End: %d\n", p.Len(), p.Start(), p.End())
	}
}

func (alloc *JudyPortionAlloc) Allocate(size uint16) (free portion.DataPortion, err error) {

	//loop over the ordered set , and find the first free portion, and slice the portion from the original part, and return
	start := uint64(size) << 40
	index, ok := alloc.sizeBasedTree.First(start)
	if ok {
		p := fromSizebasedToJudy(index)
		//p.Len() is 24bit, size is 16bit, so convert both to 32bit to compare
		if p.Len() >= uint32(size) {
			alloc.deletePortion(p)
			p, free = p.SlicePart(size)
			if p.Len() > 0 {
				alloc.addPortion(p)
			}
			return free, nil
		}
	}

	return portion.DataPortion{}, errors.Wrap(internalerror.StorageFull, "failed to alloc portion from in-memory allocator")

}

func (alloc *JudyPortionAlloc) deletePortion(p JudyPortion) {
	alloc.startBasedTree.Unset(uint64(p))
	alloc.sizeBasedTree.Unset(uint64(p.ToSizeBasedUint64()))
}

func (alloc *JudyPortionAlloc) addPortion(p JudyPortion) {
	alloc.startBasedTree.Set(uint64(p))
	alloc.sizeBasedTree.Set(uint64(p.ToSizeBasedUint64()))
}

func (alloc *JudyPortionAlloc) Release(p portion.DataPortion) {
	//check
	if alloc.isOverlapedPortion(p) {
		panic("allocate failed to allocate an overlap poriton")
	}
	free := fromDataPortionToJudy(p)
	merged := alloc.mergeFreePortions(free)
	alloc.addPortion(merged)
}

func (alloc *JudyPortionAlloc) isOverlapedPortion(p portion.DataPortion) bool {
	free := fromDataPortionToJudy(p)
	search := newJudyPortion(free.End(), 0)

	//if free's start in the prev portion
	index, ok := alloc.startBasedTree.Prev(uint64(search))
	if ok && JudyPortion(index).End() > free.Start() {
		return true
	}

	//if free's end in the next portion
	search = newJudyPortion(free.Start(), 0)
	index, ok = alloc.startBasedTree.Next(uint64(search))
	if ok && JudyPortion(index).Start() < free.End() {
		return true
	}
	return false
}

func (alloc *JudyPortionAlloc) mergeFreePortions(free JudyPortion) (merged JudyPortion) {
	merged = free
	//find the portion whose end equals to free's start
	search := newJudyPortion(free.Start(), 0)
	index, ok := alloc.startBasedTree.Prev(uint64(search))
	prePortion := JudyPortion(index)
	if ok && prePortion.End() == free.Start() {
		if prePortion.CheckedExtend(free.Len()) {
			merged = newJudyPortion(prePortion.Start(), prePortion.Len()+free.Len())
			alloc.deletePortion(prePortion)

			//used merged portion as new free portion, begin the next merge
			free = merged
		}
	}

	//find a portion whose start equals to free's end
	search = newJudyPortion(free.End(), 0)
	index, ok = alloc.startBasedTree.First(uint64(search))
	nextPortion := JudyPortion(index)
	if ok && free.End() == nextPortion.Start() {
		if free.CheckedExtend(nextPortion.Len()) {
			merged = newJudyPortion(free.Start(), free.Len()+nextPortion.Len())
			alloc.deletePortion(nextPortion)
		}
	}
	return
}

func (alloc *JudyPortionAlloc) RestoreFromIndexWithJudy(blockSize block.BlockSize,
	capacityInByte uint64, judyArray *judy.Judy1) {
	defer judyArray.Free()

	var index uint64
	var ok bool
	tail := capacityInByte / uint64(blockSize.AsU16())
	//loop for every occupied area from end to start
	index, ok = judyArray.Last(math.MaxUint64)
	for ok {
		p := JudyPortion(index)

		for p.End().AsU64() < tail {
			delta := tail - p.End().AsU64()

			size := util.Min(0xFFFFFF, delta)

			tail -= size

			start := address.AddressFromU64(tail)

			free := newJudyPortion(start, uint32(size))
			alloc.addPortion(free)

		}
		tail = p.Start().AsU64()

		index, ok = judyArray.Prev(index)
	}
}

//almost the same with RestoreFromIndex of BtreeDataPortionAlloc
func (alloc *JudyPortionAlloc) RestoreFromIndex(blockSize block.BlockSize,
	capacityInByte uint64, vec []portion.DataPortion) {
	//sort the slice reverse
	sort.Slice(vec, func(i, j int) bool {
		return vec[i].End() > vec[j].End()
	})

	tail := capacityInByte / uint64(blockSize.AsU16())

	//From end to the front
	for _, p := range vec {
		for p.End() < tail {
			delta := tail - p.End()

			size := util.Min(0xFFFFFF, delta)

			tail -= size

			start := address.AddressFromU64(tail)
			free := newJudyPortion(start, uint32(size))
			alloc.addPortion(free)
		}
		tail = p.Start.AsU64()
	}

}
