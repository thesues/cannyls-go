package allocator

import (
	"fmt"
	"sort"

	"github.com/google/btree"
	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/portion"
	"github.com/thesues/cannyls-go/util"
	"github.com/thesues/go-judy"
)

type DataPortionAlloc interface {
	Display()
	Allocate(size uint16) (free portion.DataPortion, err error)
	Release(p portion.DataPortion)
	RestoreFromIndex(blockSize block.BlockSize, capacityInByte uint64, vec []portion.DataPortion)
	MemoryUsed() uint64
	FreeCount() uint64
	GetAllocationBitStatus(n uint64, totalBlocks uint64) []float64
}

//TODO: Use ceph bitmap algorithm
type BtreeDataPortionAlloc struct {
	sizeToFree *btree.BTree
	endToFree  *btree.BTree
	freeCount  uint64
}

func (alloc *BtreeDataPortionAlloc) FreeCount() uint64 {
	return alloc.freeCount
}

func NewBtreeAlloc() *BtreeDataPortionAlloc {
	freeList := btree.NewFreeList(32)
	return &BtreeDataPortionAlloc{
		sizeToFree: btree.NewWithFreeList(32, freeList),
		endToFree:  btree.NewWithFreeList(32, freeList),
	}
}

func BuildBtreeDataPortionAlloc(capacitySector uint32) *BtreeDataPortionAlloc {
	alloc := NewBtreeAlloc()
	alloc.addFreePortion(portion.NewFreePortion(address.AddressFromU32(0), capacitySector))
	return alloc
}

func (alloc *BtreeDataPortionAlloc) MemoryUsed() uint64 {
	return 0
}

func (alloc *BtreeDataPortionAlloc) addFreePortion(free portion.FreePortion) {
	alloc.sizeToFree.ReplaceOrInsert(portion.SizeBasedPortion(free))
	alloc.endToFree.ReplaceOrInsert(portion.EndBasedPortion(free))
	alloc.freeCount += uint64(free.Len())
}

func (alloc *BtreeDataPortionAlloc) deleteFreePortion(free portion.FreePortion) {
	alloc.sizeToFree.Delete(portion.SizeBasedPortion(free))
	alloc.endToFree.Delete(portion.EndBasedPortion(free))
	alloc.freeCount -= uint64(free.Len())
}

func (alloc *BtreeDataPortionAlloc) Display() {
	fmt.Printf("==Size Based Tree==\n")
	start := portion.SizeBasedPortion(portion.FreePortion(0))
	alloc.sizeToFree.AscendGreaterOrEqual(start, func(a btree.Item) bool {
		p := portion.FreePortion(a.(portion.SizeBasedPortion))
		fmt.Printf("Portion Size: %d, Start %d, End: %d\n", p.Len(), p.Start(), p.End())
		return true
	})

	fmt.Printf("==End Based Tree==\n")
	startn := portion.EndBasedPortion(portion.FreePortion(0))
	alloc.endToFree.AscendGreaterOrEqual(startn, func(a btree.Item) bool {
		p := portion.FreePortion(a.(portion.EndBasedPortion))
		fmt.Printf("Portion Size: %d, Start %d, End: %d\n", p.Len(), p.Start(), p.End())
		return true
	})
	return
}

func (alloc *BtreeDataPortionAlloc) Allocate(size uint16) (free portion.DataPortion, err error) {
	var isAllocated = false
	start := portion.SizeBasedPortion(portion.NewFreePortion(
		address.AddressFromU32(0), uint32(size)))
	//loop over the btree, and find the first free portion, and slice the portion from the original part, and return
	alloc.sizeToFree.AscendGreaterOrEqual(start, func(a btree.Item) bool {
		p := portion.FreePortion(a.(portion.SizeBasedPortion))
		if p.Len() >= uint32(size) {
			alloc.deleteFreePortion(p)
			p, free = p.SlicePart(size)
			if p.Len() > 0 {
				alloc.addFreePortion(p)
			}
			isAllocated = true
			return false
		}
		return true
	})

	if isAllocated {
		return free, nil
	} else {
		return portion.DataPortion{},
			errors.Wrap(internalerror.StorageFull, "failed to alloc portion from in-memory allocator")
	}
}

func (alloc *BtreeDataPortionAlloc) Release(p portion.DataPortion) {
	//check
	if alloc.isOverlapedPortion(p) {
		panic("allocate failed to allocate an overlap poriton")
	}
	freeP := portion.FromDataPortion(p)
	merged := alloc.mergeFreePortions(freeP)
	alloc.addFreePortion(merged)
}

func (alloc *BtreeDataPortionAlloc) GetAllocationBitStatus(n uint64, totalBlocks uint64) []float64 {

	//create a new bitmap
	bitmap := &judy.Judy1{}
	defer bitmap.Free()
	alloc.endToFree.Ascend(func(a btree.Item) bool {
		p := portion.FreePortion(a.(portion.EndBasedPortion))
		if p.Start().AsU64() > totalBlocks {
			return false
		}
		for i := p.Start().AsU64(); i < p.End().AsU64(); i++ {
			bitmap.Set(i)
		}
		return true
	})

	/*
		Turn the bitmap to float vector
		every "n" sectors merged into one block, this block is used for render pictures
	*/
	bitmapVector := make([]float64, totalBlocks/n, totalBlocks/n)

	var i uint64
	for i = 0; i < totalBlocks/n; i++ {
		freeCounts := bitmap.CountFrom(i*n, (i+1)*n-1)
		bitmapVector[i] = float64(n-freeCounts) / float64(n)
	}
	return bitmapVector
}

func (alloc *BtreeDataPortionAlloc) isOverlapedPortion(p portion.DataPortion) bool {
	var isOverlap = false
	tmp := portion.FromDataPortion(p)
	key := portion.EndBasedPortion(portion.NewFreePortion(tmp.Start(), 0))

	//Should be alloc.endToFree.AscendGreater
	alloc.endToFree.AscendGreaterOrEqual(key, func(a btree.Item) bool {
		next := portion.FreePortion(a.(portion.EndBasedPortion))
		// Skip, if key == a for EndBasedPortion
		// tmp.Start() == key.End()
		if next.End() == tmp.Start() {
			return true
		}

		if next.Start() < tmp.End() {
			isOverlap = true
		}
		return false
	})

	return isOverlap
}

func (alloc *BtreeDataPortionAlloc) mergeFreePortions(free portion.FreePortion) (merged portion.FreePortion) {
	merged = free
	//find the a portion whose end equals to free's start
	start := portion.EndBasedPortion(portion.NewFreePortion(free.Start(), 0))
	if prevPortion := alloc.endToFree.Get(start); prevPortion != nil {
		p := portion.FreePortion(prevPortion.(portion.EndBasedPortion))
		_, ok := p.CheckedExtend(free.Len())
		//could enlarge to that big
		if ok {
			merged = portion.NewFreePortion(p.Start(), p.Len()+free.Len())
			//remove prev
			alloc.deleteFreePortion(p)

			//used merged portion as new free portion, begin the next merge
			free = merged
		}
	}

	//find a portion whose start equals to free's end
	end := portion.EndBasedPortion(portion.NewFreePortion(free.End(), 0))
	alloc.endToFree.AscendGreaterOrEqual(end, func(a btree.Item) bool {
		p := portion.FreePortion(a.(portion.EndBasedPortion))
		if p.Start() == free.End() {
			_, ok := free.CheckedExtend(p.Len())
			if ok {
				merged = portion.NewFreePortion(free.Start(), free.Len()+p.Len())
				//alloc.endToFree.Delete(portion.EndBasedPortion(p))
				alloc.deleteFreePortion(p)
			}

		}
		return false
	})

	return merged

}

func (alloc *BtreeDataPortionAlloc) RestoreFromIndex(blockSize block.BlockSize,
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
			free := portion.NewFreePortion(start, uint32(size))
			alloc.addFreePortion(free)
		}
		tail = p.Start.AsU64()
	}

}
