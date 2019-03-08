package address

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddress(t *testing.T) {
	a1 := AddressFromU64(0)
	assert.Equal(t, Address(0), a1)

	aMax := AddressFromU64(MAX_ADDRESS)
	assert.Equal(t, Address(MAX_ADDRESS), aMax)

	//panic
	assert.Panics(t, func() { AddressFromU64(MAX_ADDRESS + 1) })

	// 10 + 2 = 12
	assert.Equal(t, AddressFromU32(10).Add(AddressFromU32(2)), Address(12))

	// 0 - 5
	assert.Panics(t, func() { AddressFromU32(0).Sub(AddressFromU32(5)) })
}
