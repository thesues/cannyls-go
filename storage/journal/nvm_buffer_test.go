package journal

import (
	"testing"

	"fmt"
	"io"
	"os"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/nvm"
)

var _ = fmt.Printf
var _ = os.Open

func TestJournalNvmBufferFlush(t *testing.T) {
	f := newMemNVM()
	bufferedNvm := NewJournalNvmBuffer(f)

	n, err := bufferedNvm.Write([]byte("foo"))
	assert.Nil(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, []byte{0, 0, 0}, f.AsBytes()[:3])

	bufferedNvm.Write([]byte("bar"))
	assert.Equal(t, []byte{0, 0, 0}, f.AsBytes()[3:6])

	err = bufferedNvm.Flush()
	assert.Nil(t, err)

	assert.Equal(t, []byte("foobar"), f.AsBytes()[0:6])

}
func TestJournalNvmBufferSeekFlush(t *testing.T) {
	f := newMemNVM()
	defer f.Close()
	buffer := NewJournalNvmBuffer(f)

	//write "foo" and skip one byte , write bar
	buffer.Write([]byte("foo"))
	buffer.Seek(1, io.SeekCurrent)
	buffer.Write([]byte("bar"))

	assert.Equal(t, []byte{0, 0, 0}, f.AsBytes()[0:3])
	assert.Equal(t, []byte{0, 0, 0}, f.AsBytes()[4:7])

	buffer.Flush()
	assert.Equal(t, []byte("foo"), f.AsBytes()[0:3])
	assert.Equal(t, []byte("bar"), f.AsBytes()[4:7])

}

func TestJournalNvmBufferSeekFlush2(t *testing.T) {
	f := newMemNVM()
	defer f.Close()
	buffer := NewJournalNvmBuffer(f)

	buffer.Seek(0, io.SeekStart)
	buffer.Write([]byte("foo"))
	buffer.Seek(512, io.SeekStart)
	buffer.Write([]byte("bar"))

	assert.Equal(t, []byte{0, 0, 0}, f.AsBytes()[0:3])
	assert.Equal(t, []byte{0, 0, 0}, f.AsBytes()[512:515])
	buffer.Flush()
	assert.Equal(t, []byte("foo"), f.AsBytes()[0:3])
	assert.Equal(t, []byte("bar"), f.AsBytes()[512:515])

}

func TestJournalNvmBufferSeekFlush3(t *testing.T) {
	//Test Seek backwards
	f := newMemNVM()
	defer f.Close()
	buffer := NewJournalNvmBuffer(f)

	buffer.Write([]byte("foo"))
	buffer.Seek(-1, io.SeekCurrent)

	buffer.Write([]byte("bar"))

	assert.Equal(t, []byte{0, 0, 0, 0, 0}, f.AsBytes()[0:5])
	buffer.Flush()

	assert.Equal(t, []byte("fobar"), f.AsBytes()[0:5])
}

func TestJournalNvmBufferAutoFlush(t *testing.T) {
	f := newMemNVM()
	defer f.Close()
	buffer := NewJournalNvmBuffer(f)

	//if read the dirty region, flush first, and read from disk
	buffer.Write([]byte("foo"))
	assert.Equal(t, []byte{0, 0, 0}, f.AsBytes()[0:3])

	var buf [1]byte
	_, err := buffer.Read(buf[:])
	assert.Nil(t, err)
	assert.Equal(t, []byte("foo"), f.AsBytes()[:3])
}

func TestJournalNvmBufferAutoFlush1(t *testing.T) {
	f := newMemNVM()
	defer f.Close()
	buffer := NewJournalNvmBuffer(f)

	buffer.Write([]byte("foo"))
	assert.Equal(t, []byte{0, 0, 0}, f.AsBytes()[0:3])

	n, err := buffer.Seek(512, io.SeekStart)
	assert.Equal(t, int64(512), n)
	assert.Nil(t, err)

	var buf [1]byte
	_, err = buffer.Read(buf[:])
	assert.Nil(t, err)
	assert.Equal(t, []byte{0, 0, 0}, f.AsBytes()[:3])

}

func TestJournalNvmBufferOverwrite(t *testing.T) {
	f := newMemNVM()
	defer f.Close()
	buffer := NewJournalNvmBuffer(f)

	dataA := newSliceWithValue(512, 'A')
	_, err := buffer.Write(dataA)
	assert.Nil(t, err)

	_, err = buffer.Seek(256, io.SeekStart)
	assert.Nil(t, err)

	_, err = buffer.Write([]byte{'B'})
	assert.Nil(t, err)
	buffer.Flush()

	assert.Equal(t, dataA[:256], f.AsBytes()[0:256])
	assert.Equal(t, byte('B'), f.AsBytes()[256])

}

//helper
func newFileNVM(path string) *nvm.FileNVM {
	file, _ := nvm.CreateIfAbsent(path, 10*1024)
	return file
}

func newMemNVM() *nvm.MemoryNVM {
	m, _ := nvm.New(10 * 1024)
	return m
}

func newSliceWithValue(size int, initial byte) []byte {
	n := make([]byte, size)
	for i := range n {
		n[i] = initial
	}
	return n
}

func TestJournalNvmBufferSeekBUG(t *testing.T) {
	f := newMemNVM()
	defer f.Close()
	buffer := NewJournalNvmBuffer(f)

	buffer.Seek(512, io.SeekStart)
	buffer.Write([]byte("bar"))

	buffer.Seek(0, io.SeekStart)
	x := make([]byte, 513)
	copy(x, "foo")
	x[512] = 'z'
	buffer.Write(x)

	buffer.Flush()
	assert.Equal(t, []byte("foo"), f.AsBytes()[0:3])
	assert.Equal(t, []byte("zar"), f.AsBytes()[512:515])

}
