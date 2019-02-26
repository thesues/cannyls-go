package block

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlockCeilAlign(t *testing.T) {
	bs, err := NewBlockSize(512)
	assert.Equal(t, err, nil)
	assert.Equal(t, bs.CeilAlign(10), uint64(512))
	assert.Equal(t, bs.CeilAlign(513), uint64(1024))
	assert.Equal(t, bs.CeilAlign(1024), uint64(1024))
}

func TestBlockFloorAlign(t *testing.T) {
	bs, err := NewBlockSize(512)
	assert.Equal(t, err, nil)
	assert.Equal(t, bs.FloorAlign(10), uint64(0))
	assert.Equal(t, bs.FloorAlign(513), uint64(512))
	assert.Equal(t, bs.FloorAlign(1024), uint64(1024))
}

func TestBlockIsAligned(t *testing.T) {
	bs, err := NewBlockSize(512)
	assert.Equal(t, err, nil)

	assert.Equal(t, bs.IsAligned(0), true)
	assert.Equal(t, bs.IsAligned(512), true)
	assert.Equal(t, bs.IsAligned(1024), true)
	assert.Equal(t, bs.IsAligned(511), false)
	assert.Equal(t, bs.IsAligned(513), false)
}

func TestBlockContains(t *testing.T) {
	bs, err := NewBlockSize(2048)
	assert.Equal(t, err, nil)

	bs1024, _ := NewBlockSize(1024)
	bs512, _ := NewBlockSize(512)
	bs1536, _ := NewBlockSize(1536)
	assert.Equal(t, bs.Contains(bs1024), true)
	assert.Equal(t, bs.Contains(bs512), true)
	assert.Equal(t, bs.Contains(bs1536), false)
}
