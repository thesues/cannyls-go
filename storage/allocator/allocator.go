package allocator

import (
	"fmt"
	"github.com/google/btree"
	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/portion"
)

//TODO: Use ceph bitmap algorithm
type DataPortionAllocator struct {
	sizeToFree *btree.BTree
	endToFree  *btree.BTree
}

func New() *DataPortionAllocator {
	freeList := btree.NewFreeList(32)
	return &DataPortionAllocator{
		sizeToFree: btree.NewWithFreeList(2, freeList),
		endToFree:  btree.NewWithFreeList(2, freeList),
	}
}

func Build(capacitySector uint32) *DataPortionAllocator {
	alloc := New()
	alloc.addFreePortion(portion.New(address.AddressFromU32(0), capacitySector))
	return alloc
}

func (alloc *DataPortionAllocator) addFreePortion(free portion.FreePortion) {
	alloc.sizeToFree.ReplaceOrInsert(portion.SizeBasedPortion(free))
	alloc.endToFree.ReplaceOrInsert(portion.EndBasedPortion(free))
}

func (alloc *DataPortionAllocator) DeleteFreePortion(free portion.FreePortion) {
	alloc.sizeToFree.Delete(portion.SizeBasedPortion(free))
	alloc.endToFree.Delete(portion.EndBasedPortion(free))
}

func (alloc *DataPortionAllocator) Display() {
	fmt.Printf("==Size Based Tree==\n")
	start := portion.SizeBasedPortion(portion.DefaultFreePortion())
	alloc.sizeToFree.AscendGreaterOrEqual(start, func(a btree.Item) bool {
		p := portion.FreePortion(a.(portion.SizeBasedPortion))
		fmt.Printf("Portion Size: %d, Start %d, End: %d\n", p.Len(), p.Start(), p.End())
		return true
	})

	fmt.Printf("==End Based Tree==\n")
	startn := portion.EndBasedPortion(portion.DefaultFreePortion())
	alloc.endToFree.AscendGreaterOrEqual(startn, func(a btree.Item) bool {
		p := portion.FreePortion(a.(portion.EndBasedPortion))
		fmt.Printf("Portion Size: %d, Start %d, End: %d\n", p.Len(), p.Start(), p.End())
		return true
	})
	return
}

func (alloc *DataPortionAllocator) Allocate(size uint16) (free portion.DataPortion, err error) {
	var isAllocated = false
	start := portion.SizeBasedPortion(portion.New(address.AddressFromU32(0), uint32(size)))
	//loop over the btree, and find the first free portion, and slice the portion from the original part, and return
	alloc.sizeToFree.AscendGreaterOrEqual(start, func(a btree.Item) bool {
		p := portion.FreePortion(a.(portion.SizeBasedPortion))
		if p.Len() >= uint32(size) {
			alloc.DeleteFreePortion(p)
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

func (alloc *DataPortionAllocator) Release(p portion.DataPortion) {
	//check
	if alloc.isOverlapedPortion(p) {
		panic("allocate failed to allocate an overlap poriton")
	}
	freeP := portion.FromDataPortion(p)
	merged := alloc.mergeFreePortions(freeP)
	alloc.addFreePortion(merged)
}

func (alloc *DataPortionAllocator) isOverlapedPortion(p portion.DataPortion) bool {
	var isOverlap = false
	tmp := portion.FromDataPortion(p)
	key := portion.EndBasedPortion(portion.New(tmp.Start(), 0))

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

func (alloc *DataPortionAllocator) mergeFreePortions(free portion.FreePortion) (merged portion.FreePortion) {
	merged = free
	//find the a portion whose end equals to free's start
	start := portion.EndBasedPortion(portion.New(free.Start(), 0))
	if prevPortion := alloc.endToFree.Get(start); prevPortion != nil {
		p := portion.FreePortion(prevPortion.(portion.EndBasedPortion))
		_, ok := p.CheckedExtend(free.Len())
		//could enlarge to that big
		if ok {
			merged = portion.New(p.Start(), p.Len()+free.Len())
			//remove prev
			alloc.DeleteFreePortion(p)

			//used merged portion as new free portion, begin the next merge
			free = merged
		}
	}

	//find a portion whose start equals to free's end
	end := portion.EndBasedPortion(portion.New(free.End(), 0))
	alloc.endToFree.AscendGreaterOrEqual(end, func(a btree.Item) bool {
		p := portion.FreePortion(a.(portion.EndBasedPortion))
		if p.Start() == free.End() {
			_, ok := free.CheckedExtend(p.Len())
			if ok {
				merged = portion.New(free.Start(), free.Len()+p.Len())
				//alloc.endToFree.Delete(portion.EndBasedPortion(p))
				alloc.DeleteFreePortion(p)
			}

		}
		return false
	})

	return merged

}
