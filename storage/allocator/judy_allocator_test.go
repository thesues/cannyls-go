package allocator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lumpindex"
)

func TestJudyPortionCompare(t *testing.T) {
	//compare by end_address
	p1 := newJudyPortion(address.AddressFromU64(0), 100)  //0, 100
	p2 := newJudyPortion(address.AddressFromU64(0), 200)  //257, 200
	p3 := newJudyPortion(address.AddressFromU64(400), 10) //400, 10
	p4 := newJudyPortion(address.AddressFromU64(401), 6)  //410, 6

	assert.True(t, p2 > p1)
	assert.True(t, p3 > p2)
	assert.True(t, p4 > p3)

	assert.True(t, p1.ToSizeBasedUint64() < p2.ToSizeBasedUint64())
	assert.True(t, p2.ToSizeBasedUint64() > p3.ToSizeBasedUint64())
	assert.True(t, p2.ToSizeBasedUint64() > p1.ToSizeBasedUint64())
	assert.True(t, p3.ToSizeBasedUint64() > p4.ToSizeBasedUint64())

}

func TestAllocatorJudyRestore(t *testing.T) {
	index := lumpindex.NewIndex()
	index.InsertDataPortion(lumpidnum(1), fportion(0, 10))
	index.InsertDataPortion(lumpidnum(2), fportion(4*512, 10000))
	index.InsertDataPortion(lumpidnum(3), fportion(10+(1<<24), 10))

	var sizeInBytes uint64 = (30 + (1 << 24)) * 512
	j := index.JudyDataPortions()

	jalloc := NewJudyAlloc()
	jalloc.RestoreFromIndexWithJudy(block.Min(), sizeInBytes, j)
	//jalloc.Display()

	//fmt.Println("GOOD:")
	pd := index.DataPortions()
	balloc := NewBtreeAlloc()
	balloc.RestoreFromIndex(block.Min(), sizeInBytes, pd)
	//balloc.Display()

	/*
		The output of the above test case:, check each number below
		==Size Based Tree==
		Portion Size: 10, Start 16777236, End: 16777246
		Portion Size: 2038, Start 10, End: 2048
		Portion Size: 16765178, Start 12048, End: 16777226
		==Start Based Tree==
		Portion Size: 2038, Start 10, End: 2048
		Portion Size: 16765178, Start 12048, End: 16777226
		Portion Size: 10, Start 16777236, End: 16777246
	*/
	sizeBased, startBased := jalloc.allPortions()
	assert.Equal(t, 3, int(len(startBased)))
	assert.Equal(t, 3, int(len(startBased)))

	//line 1
	assert.Equal(t, uint32(10), sizeBased[0].Len())
	assert.Equal(t, uint64(16777236), sizeBased[0].Start().AsU64())

	//line 2
	assert.Equal(t, uint32(2038), sizeBased[1].Len())
	assert.Equal(t, uint64(10), sizeBased[1].Start().AsU64())

	//line 3
	assert.Equal(t, uint32(16765178), sizeBased[2].Len())
	assert.Equal(t, uint64(12048), sizeBased[2].Start().AsU64())

	//line4
	assert.Equal(t, uint32(2038), startBased[0].Len())
	assert.Equal(t, uint64(10), startBased[0].Start().AsU64())

	//line5
	assert.Equal(t, uint32(16765178), startBased[1].Len())
	assert.Equal(t, uint64(12048), startBased[1].Start().AsU64())

	//line6
	assert.Equal(t, uint32(10), startBased[2].Len())
	assert.Equal(t, uint64(16777236), startBased[2].Start().AsU64())

}
