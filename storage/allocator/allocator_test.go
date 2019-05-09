package allocator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/portion"
)

func TestAllocateBTree(t *testing.T) {
	alloc := BuildBtreeDataPortionAlloc(24)
	DoTestAllocate(t, alloc)
}

func TestAllocateJudy(t *testing.T) {
	alloc := BuildJudyAlloc(24)
	DoTestAllocate(t, alloc)
}

func DoTestAllocate(t *testing.T, alloc DataPortionAlloc) {
	p, err := alloc.Allocate(10)
	assert.Nil(t, err)
	assert.Equal(t, fportion(0, 10), p)

	p, err = alloc.Allocate(10)
	assert.Nil(t, err)
	assert.Equal(t, fportion(10, 10), p)

	p, err = alloc.Allocate(10)
	assert.Error(t, err)

	p, err = alloc.Allocate(4)
	assert.Nil(t, err)
	assert.Equal(t, fportion(20, 4), p)

	alloc.Release(fportion(10, 10))

	p, err = alloc.Allocate(5)
	assert.Nil(t, err)
	assert.Equal(t, fportion(10, 5), p)
	p, err = alloc.Allocate(2)
	assert.Nil(t, err)
	assert.Equal(t, fportion(15, 2), p)

	p, err = alloc.Allocate(4)
	assert.Error(t, err)

}

func TestAllocateBTreeShouldPanic(t *testing.T) {
	alloc := BuildBtreeDataPortionAlloc(24)
	DoTestAllocateShouldPanic(t, alloc)
}
func TestAllocateJudyShouldPanic(t *testing.T) {
	alloc := BuildJudyAlloc(24)
	DoTestAllocateShouldPanic(t, alloc)
}

func DoTestAllocateShouldPanic(t *testing.T, alloc DataPortionAlloc) {
	assert.Panics(t, func() {
		alloc.Release(fportion(10, 10))
	})
}

func TestAllocateBTreeRelease(t *testing.T) {
	alloc := BuildBtreeDataPortionAlloc(419431)
	DoTestAllocateRelease(t, alloc)
}
func TestAllocateJudyRelease(t *testing.T) {
	alloc := BuildJudyAlloc(419431)
	DoTestAllocateRelease(t, alloc)
}

func DoTestAllocateRelease(t *testing.T, alloc DataPortionAlloc) {
	var p0, p1, p2, p3, p4, p5, p6 portion.DataPortion
	var err error
	p0, err = alloc.Allocate(65)
	assert.Nil(t, err)
	p1, err = alloc.Allocate(65)
	assert.Nil(t, err)
	p2, err = alloc.Allocate(65)
	assert.Nil(t, err)
	alloc.Release(p0)
	alloc.Release(p1)

	p3, err = alloc.Allocate(65)
	assert.Nil(t, err)
	p4, err = alloc.Allocate(65)
	assert.Nil(t, err)

	alloc.Release(p3)
	alloc.Release(p4)
	alloc.Release(p2)

	p5, err = alloc.Allocate(65)
	p6, err = alloc.Allocate(65)
	alloc.Release(p5)
	alloc.Release(p6)
	alloc.Display()
}

func fportion(addr uint64, size uint16) portion.DataPortion {
	return portion.NewDataPortion(addr, size)
}
