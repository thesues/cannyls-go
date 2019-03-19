package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/storage/allocator"
	"testing"
)

func Test_putUint16BigEndian(t *testing.T) {
	var buf [2]byte
	putUint16BigEndian(buf[:], 0x1234)
	assert.Equal(t, byte(0x12), buf[0])
	assert.Equal(t, byte(0x34), buf[1])

	r := getUint16BigEndion(buf[:])
	assert.Equal(t, uint16(0x1234), r)
}

func TestDataRegion(t *testing.T) {
	var capacity_bytes uint32 = 10 * 1024
	alloc := allocator.Build(capacity_bytes / uint32(512))
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
