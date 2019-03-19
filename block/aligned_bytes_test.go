package block

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAlignedBytesNew(t *testing.T) {
	//new
	aligned := NewAlignedBytes(10, Min())
	assert.Equal(t, uint32(10), aligned.Len())
	assert.Equal(t, uint64(512), aligned.Capacity())

	//from_bytes
	aligned = FromBytes([]byte{1, 2, 3}, Min())
	assert.Equal(t, uint32(3), aligned.Len())
	assert.Equal(t, uint64(512), aligned.Capacity())
	assert.Equal(t, []byte{1, 2, 3}, aligned.AsBytes())
}

func TestAlignedBytesAlign(t *testing.T) {

	//align
	aligned := NewAlignedBytes(10, Min())
	assert.Equal(t, uint32(10), aligned.Len())

	aligned.Align()
	assert.Equal(t, uint32(512), aligned.Len())

	aligned.Align()
	assert.Equal(t, uint32(512), aligned.Len())

}
func TestAlignedBytesTruncate(t *testing.T) {
	//trucate
	aligned := NewAlignedBytes(10, Min())
	assert.Equal(t, uint32(10), aligned.Len())

	//success
	aligned.Truncate(2)
	assert.Equal(t, uint32(2), aligned.Len())

	//fail
	aligned.Truncate(3)
	assert.Equal(t, uint32(2), aligned.Len())

}
func TestAlignedBytesResize(t *testing.T) {
	aligned := NewAlignedBytes(10, Min())

	aligned.Resize(100)
	assert.Equal(t, uint32(100), aligned.Len())
	assert.Equal(t, uint64(512), aligned.Capacity())

	aligned.Resize(10)
	assert.Equal(t, uint32(10), aligned.Len())

	aligned.AlignResize(513)
	assert.Equal(t, uint32(1024), aligned.Len())

}

func TestAligneBytesOffset(t *testing.T) {
	var i = 511
	for i < 1000 {
		NewAlignedBytes(i, Min())
		i += 1
	}
}
