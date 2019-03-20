package util

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_putUint16BigEndian(t *testing.T) {
	var buf [2]byte
	PutUint16BigEndian(buf[:], 0x1234)
	assert.Equal(t, byte(0x12), buf[0])
	assert.Equal(t, byte(0x34), buf[1])

	r := GetUint16BigEndion(buf[:])
	assert.Equal(t, uint16(0x1234), r)
}
