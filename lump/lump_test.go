package lump

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
