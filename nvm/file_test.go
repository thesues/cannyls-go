package nvm

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
	"io"
	"os"
	"syscall"
	"testing"
)

func TestFileNVMOpen(t *testing.T) {

	//open an non-exist file
	nvm, err := Open("foo-test.lusf")
	assert.Error(t, err)
	assert.Nil(t, nvm)
}

func TestFileNVMReopen(t *testing.T) {
	//create a new file
	nvm, err := CreateIfAbsent("foo-test.lusf", 10*1024)
	assert.Nil(t, err)

	defer os.Remove("foo-test.lusf")

	data := bytes.NewBuffer([]byte{})
	err = DefaultStorageHeader().WriteTo(data)
	assert.Nil(t, err)

	headerSize := data.Len()

	data.Write([]byte("bar"))

	n, err := nvm.Write(align(data.Bytes()))

	assert.Equal(t, block.Min().CeilAlign(uint64(headerSize)), uint64(n))
	assert.Nil(t, err)

	//open the file will fail, because the exclusive lock
	_, err = Open("foo-test.lusf")
	assert.Error(t, err)

	_, err = CreateIfAbsent("foo-test.lusf", 1024*10)
	assert.Error(t, err)

	nvm.Close()

	nvm, err = Open("foo-test.lusf")
	assert.Nil(t, err)

	/*
		Now foo-test.lusf should be
		Header Messages + "bar" in the first sector
	*/

	ab := block.NewAlignedBytes(headerSize+3, block.Min()).Align().AsBytes()

	n, err = nvm.Read(ab)

	assert.Equal(t, 512, n)
	assert.Equal(t, []byte("bar"), ab[headerSize:headerSize+3])

	//assert_eq!(&buf[header_len..][..3], b"bar");

}

func TestFileNVMSimpleCreate(t *testing.T) {

	nvm, err := CreateIfAbsent("foo", 10*1024)
	assert.Nil(t, err)
	defer os.Remove("foo")

	buf := new(bytes.Buffer)
	DefaultStorageHeader().WriteTo(buf)
	//copy memory
	ab := align(buf.Bytes())
	_, err = nvm.Write(ab)
	assert.Nil(t, err)
	nvm.Close()

	//check foo is created
	_, err = os.Stat("foo")
	assert.Nil(t, err)

	nvm, err = CreateIfAbsent("foo", 10*1024)

	assert.Nil(t, err)

	readbuf := make([]byte, 512)
	_, err = nvm.Read(readbuf)
	assert.Nil(t, err)
	assert.Equal(t, ab, readbuf)
}

func TestFileNVMWrite(t *testing.T) {
	nvm, err := CreateIfAbsent("foo", 1024)
	assert.Nil(t, err)
	os.Remove("foo")

	n, err := nvm.Write(alignedWithSize(2048))
	fmt.Println(n)
	assert.Nil(t, err)

}

func TestFileNVMOperations(t *testing.T) {
	nvm, err := CreateIfAbsent("foo", 1024)
	assert.Nil(t, err)
	defer os.Remove("foo")

	assert.Equal(t, uint64(1024), nvm.Capacity())
	assert.Equal(t, uint64(0), nvm.Position())

	// read, write, seek
	buf := alignedWithSize(512)
	n, err := nvm.Read(buf)

	assert.Nil(t, err)
	assert.Equal(t, 512, n)
	assert.Equal(t, buf, arrayWithValueSize(512, 0))
	assert.Equal(t, uint64(512), nvm.Position())

	array := arrayWithValueSize(512, 1)
	_, err = nvm.Write(align(array))
	assert.Nil(t, err)
	assert.Equal(t, uint64(1024), nvm.Position())

	nvm.Seek(512, io.SeekStart)
	assert.Equal(t, uint64(512), nvm.Position())

	n, err = nvm.Read(buf)
	assert.Nil(t, err)
	assert.Equal(t, uint64(1024), nvm.Position())
	assert.Equal(t, 512, n)
	assert.Equal(t, array, buf)

	left, right, err := nvm.Split(512)
	assert.Nil(t, err)
	assert.Equal(t, uint64(512), left.Capacity())
	left.Seek(0, io.SeekStart)

	var readBuf [512]byte
	//left
	left.Read(readBuf[:])
	assert.Equal(t, arrayWithValueSize(512, 0), readBuf[:])

	//right
	assert.Equal(t, uint64(512), right.Capacity())
	right.Seek(0, io.SeekStart)
	right.Read(readBuf[:])
	assert.Equal(t, arrayWithValueSize(512, 1), readBuf[:])

}

func TestFileNVMDirectIO(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-dio", 1024)
	defer os.Remove("foo-dio")
	//
	flag, err := fcntl(int(nvm.file.Fd()), syscall.F_GETFL, 0)
	assert.Nil(t, err)
	assert.Equal(t, true, isDirectIO(flag))
	nvm.Close()
}

func TestFileNVMEXLock(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-dio", 1024)
	assert.Nil(t, err)
	defer os.Remove("foo-dio")

	flag, err := fcntl(int(nvm.file.Fd()), syscall.F_GETFL, 0)
	assert.Nil(t, err)
	assert.Equal(t, true, isExclusiveLock("foo-dio", flag))

	nvm.Close()

}

//helper function
func align(bytes []byte) []byte {
	ab := block.FromBytes(bytes, block.Min())
	return ab.Align().AsBytes()
}

func alignedWithSize(size int) []byte {
	ab := block.NewAlignedBytes(size, block.Min())

	return ab.Align().AsBytes()
}

func arrayWithValueSize(size int, value byte) []byte {
	arr := make([]byte, size)
	for i := range arr {
		arr[i] = value
	}
	return arr
}
