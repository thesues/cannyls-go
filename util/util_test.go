package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_putUint16(t *testing.T) {
	var buf [2]byte
	PutUINT16(buf[:], 0x1234)
	assert.Equal(t, byte(0x12), buf[0])
	assert.Equal(t, byte(0x34), buf[1])

	r := GetUINT16(buf[:])
	assert.Equal(t, uint16(0x1234), r)

	var buf1 [5]byte
	PutUINT40(buf1[:], 0x1234567890)
	assert.Equal(t, byte(0x12), buf1[0])
	assert.Equal(t, byte(0x90), buf1[4])

	r40bit := GetUINT40(buf1[:])
	assert.Equal(t, uint64(0x1234567890), r40bit)

}
