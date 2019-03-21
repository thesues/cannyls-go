package lump

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLumpIDFromString(t *testing.T) {
	_, err := FromString("111")
	assert.Nil(t, err)
}

func TestLumpID(t *testing.T) {
	lid, err := FromString("A1")
	assert.Nil(t, err)
	assert.Equal(t, "000000000000000000000000000000a1", lid.String())

	left := FromU64(0, 10)

	right, err := FromString("0A")
	assert.Nil(t, err)

	var buf [16]byte
	buf[15] = 10
	mid, err := FromBytes(buf[:])
	assert.Nil(t, err)

	assert.Equal(t, 0, left.Compare(right))
	assert.Equal(t, 0, right.Compare(mid))
	assert.Equal(t, 0, right.Compare(left))

}
