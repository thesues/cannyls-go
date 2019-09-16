package lump

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
)

func TestLumpIDFromString(t *testing.T) {
	_, err := FromString("1111")
	assert.Nil(t, err)
}

func TestLumpID(t *testing.T) {
	lid, err := FromString("A1")
	assert.Nil(t, err)
	assert.Equal(t, "a1", lid.String())

	left := FromU64(0, 10)

	right, err := FromString("0A")
	assert.Nil(t, err)

	var buf [8]byte
	buf[7] = 10
	mid, err := FromBytes(buf[:])
	assert.Nil(t, err)

	assert.Equal(t, 0, left.Compare(right))
	assert.Equal(t, 0, right.Compare(mid))
	assert.Equal(t, 0, right.Compare(left))

}

func BenchmarkLump(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ab := NewLumpDataAligned(16<<10, block.Min())
		buf := ab.AsBytes()
		for i := 0; i < len(buf); i++ {
			buf[i] = 0
		}
	}
}

func BenchmarkLumpWithPool(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		ab := GetLumpData(16 << 10)
		buf := ab.AsBytes()
		for i := 0; i < len(buf); i++ {
			buf[i] = 0
		}
		PutLumpData(ab)
	}
}
