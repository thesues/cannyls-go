package nvm

import (
	_ "fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
)

func TestMemory(t *testing.T) {
	nvm, _ := New(1024)
	assert.Equal(t, uint64(0), nvm.Position())

	//read, write, seek
	buf := make([]byte, 512)
	nvm.Read(buf)
	assert.Equal(t, buf, nvm.vec[:512])
	assert.Equal(t, uint64(512), nvm.Position())

	for i := range buf {
		buf[i] = 1
	}

	n, err := nvm.Write(buf)
	assert.Equal(t, 512, n)
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(1024), nvm.Position())

	_, err = nvm.Seek(512, io.SeekStart)
	assert.Equal(t, nil, err)
	assert.Equal(t, uint64(512), nvm.Position())

	readBuf := make([]byte, 512)
	_, err = nvm.Read(readBuf)
	assert.Equal(t, nil, err)
	assert.Equal(t, buf, readBuf)

	leftNVM, rightNVM, err := nvm.Split(512)
	assert.Equal(t, nil, err)

	assert.Equal(t, uint64(512), leftNVM.Capacity())
	_, err = leftNVM.Seek(0, io.SeekStart)
	assert.Equal(t, nil, err)
	_, err = leftNVM.Read(buf)
	assert.Equal(t, buf, newBuffer(512, 0))
	assert.Equal(t, uint64(512), leftNVM.Position())
	n, err = leftNVM.Read(buf)
	assert.Equal(t, io.EOF, err)

	assert.Equal(t, uint64(512), rightNVM.Capacity())
	_, err = rightNVM.Seek(0, io.SeekStart)
	assert.Equal(t, nil, err)
	_, err = rightNVM.Read(buf)
	assert.Equal(t, buf, newBuffer(512, 1))
	assert.Equal(t, uint64(512), rightNVM.Position())
	n, err = rightNVM.Read(buf)
	assert.Equal(t, io.EOF, err)

}

func newBuffer(size int, initial byte) []byte {
	n := make([]byte, size)
	for i := range n {
		n[i] = initial
	}
	return n
}
