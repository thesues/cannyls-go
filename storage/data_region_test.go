package storage

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/storage/allocator"
)

func TestDataRegion(t *testing.T) {
	var capacity_bytes uint32 = 10 * 1024
	//Use Judy allocator as default
	alloc := allocator.BuildJudyAlloc(capacity_bytes / uint32(512))
	nvm, err := nvm.New(uint64(capacity_bytes))
	assert.Nil(t, err)
	region := NewDataRegion(alloc, nvm)
	put_lump_data := lump.NewLumpDataAligned(3, block.Min())
	copy(put_lump_data.AsBytes(), []byte("foo"))
	p, err := region.Put(put_lump_data)
	assert.Nil(t, err)
	fmt.Println(p.Display())

	get_lump_data, err := region.Get(p)
	assert.Equal(t, uint32(3), get_lump_data.Inner.Len())
	assert.Nil(t, err)
	assert.Equal(t, []byte("foo"), get_lump_data.AsBytes())
}

func TestGetDataRegionWithOffset(t *testing.T) {
	//allocate 10K region
	var capacity_bytes uint32 = 10 * 1024
	//Use Judy allocator as default
	alloc := allocator.BuildJudyAlloc(capacity_bytes / uint32(512))
	nvm, err := nvm.New(uint64(capacity_bytes))
	assert.Nil(t, err)

	region := NewDataRegion(alloc, nvm)

	putLumpData := lump.NewLumpDataAligned(510*3, block.Min())
	setRandStringBytes(putLumpData.AsBytes())

	dataPortion, err := region.Put(putLumpData)
	resultAB, err := region.Get(dataPortion)
	assert.Nil(t, err)
	//The Put method will resize lumpData, so we have to resize it back
	putLumpData.Inner.Resize(1530)
	assert.Equal(t, putLumpData, resultAB)

	//first block
	resultBytes, err := region.GetWithOffset(dataPortion, 10, 500)
	assert.Nil(t, err)
	assert.Equal(t, putLumpData.Inner.AsBytes()[10:10+500], resultBytes)
	//first 2 block
	resultBytes, err = region.GetWithOffset(dataPortion, 10, 600)
	assert.Equal(t, putLumpData.Inner.AsBytes()[10:10+600], resultBytes)
	//last block
	resultBytes, err = region.GetWithOffset(dataPortion, 1500, 30)
	assert.Equal(t, putLumpData.Inner.AsBytes()[1500:1500+30], resultBytes)

	//length is bigger than object itself
	resultBytes, err = region.GetWithOffset(dataPortion, 1500, 100)
	assert.Error(t, err)

	//read to the last block
	resultBytes, err = region.GetWithOffset(dataPortion, 0, 1530)
	assert.Nil(t, err)
	assert.Equal(t, putLumpData.Inner.AsBytes()[0:1530], resultBytes)

}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func setRandStringBytes(data []byte) {
	letterBytes := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for i := range data {
		data[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
}
