package block

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReuseMemory(t *testing.T) {
	count := 0
	n := 50000
	for i := 0; i < n; i++ {
		x := make([]byte, 4000)
		_, isNew := fromBytes(x, Min())
		if !isNew {
			count++
		}
	}
	fmt.Printf("the reuse percent is %0.2f%%\n", float32(count)/float32(n)*100)

}
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
