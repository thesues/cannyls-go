package allocator

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/address"
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

func TestJudyPortionRestore(t *testing.T) {

}
