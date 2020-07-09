package nvm

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/util"
)

func BenchmarkFileThreads(b *testing.B) {
	buf := alignedWithSize(512)
	buf[511] = 10
	f, err := openFileWithDirectIO("./normal-file", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err.Error())
	}
	defer os.Remove("./normal-file")

	for i := 0; i < b.N; i++ {
		stopper := util.NewStopper()
		for j := 0; j < 10; j++ {
			index := j
			stopper.RunWorker(func() {
				f.WriteAt(buf, int64(index)*512)
			})
		}
		stopper.Wait()
		f.Sync()
	}
	f.Close()
}

func BenchmarkFile(b *testing.B) {
	buf := alignedWithSize(512)
	buf[511] = 10
	f, err := openFileWithDirectIO("./normal-file", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err.Error())
	}
	defer os.Remove("./normal-file")
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			_, err = f.Write(buf)
			if err != nil {
				panic(err.Error())
			}
		}
		f.Sync()
	}
	f.Close()
}

func BenchmarkNVM(b *testing.B) {
	buf := alignedWithSize(512)
	buf[511] = 10
	//create a new file
	nvm, err := CreateIfAbsent("foo-test.lusf", 10*1024*1024, true)
	if err != nil {
		panic(err.Error())
	}
	defer os.Remove("foo-test.lusf")

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			_, err = nvm.Write(buf)
			if err != nil {
				panic(err.Error())
			}
		}
		nvm.Sync()
	}
	nvm.Close()

}

func TestFileNVMOpen(t *testing.T) {

	//open an non-exist file
	nvm, _, err := Open("foo-test.lusf", true)
	assert.Error(t, err)
	assert.Nil(t, nvm)
}

func TestFileNVMReopen(t *testing.T) {
	//create a new file
	nvm, err := CreateIfAbsent("foo-test.lusf", 10*1024, true)
	assert.Nil(t, err)

	data := new(bytes.Buffer)
	err = DefaultStorageHeader().WriteTo(data)
	assert.Nil(t, err)
	nvm.Write(align(data.Bytes()))
	assert.Nil(t, err)
	nvm.Sync()
	nvm.Close()

	defer os.Remove("foo-test.lusf")

	nvm, _, err = Open("foo-test.lusf", true)
	fmt.Printf("%+v\n", err)
	assert.Nil(t, err)

	data = bytes.NewBuffer([]byte{})
	err = DefaultStorageHeader().WriteTo(data)
	assert.Nil(t, err)

	headerSize := data.Len()

	data.Write([]byte("bar"))

	n, err := nvm.Write(align(data.Bytes()))

	assert.Equal(t, block.Min().CeilAlign(uint64(headerSize)), uint64(n))
	assert.Nil(t, err)

	//open the file will fail, because the exclusive lock
	_, _, err = Open("foo-test.lusf", true)
	assert.Error(t, err)

	_, err = CreateIfAbsent("foo-test.lusf", 1024*10, true)
	assert.Error(t, err)

	nvm.Close()

	nvm, _, err = Open("foo-test.lusf", true)
	assert.Nil(t, err)

	ab := block.NewAlignedBytes(headerSize+3, block.Min()).Align().AsBytes()

	n, err = nvm.Read(ab)

	assert.Equal(t, 512, n)
	assert.Equal(t, []byte("bar"), ab[headerSize:headerSize+3])

}

func TestFileNVMSimpleCreate(t *testing.T) {

	nvm, err := CreateIfAbsent("foo.lusf", 10*1024, true)
	assert.Nil(t, err)
	defer os.Remove("foo.lusf")

	buf := new(bytes.Buffer)
	DefaultStorageHeader().WriteTo(buf)
	//copy memory
	ab := align(buf.Bytes())
	_, err = nvm.Write(ab)
	assert.Nil(t, err)
	nvm.Close()

	//check foo is created
	_, err = os.Stat("foo.lusf")
	assert.Nil(t, err)

	nvm, _, err = Open("foo.lusf", true)

	assert.Nil(t, err)

	readbuf := make([]byte, 512)
	_, err = nvm.Read(readbuf)
	assert.Nil(t, err)
	assert.Equal(t, ab, readbuf)
}

func TestFileNVMWrite(t *testing.T) {
	nvm, err := CreateIfAbsent("foo.lusf", 1024, true)
	assert.Nil(t, err)
	os.Remove("foo.lusf")

	n, err := nvm.Write(alignedWithSize(2048))
	fmt.Println(n)
	assert.Nil(t, err)

}

func TestFileNVMOperations(t *testing.T) {
	nvm, err := CreateIfAbsent("foo.lusf", 1024, true)
	assert.Nil(t, err)
	defer os.Remove("foo.lusf")

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
	nvm, err := CreateIfAbsent("foo-dio.lusf", 1024, true)
	defer os.Remove("foo-dio.lusf")

	data := new(bytes.Buffer)
	err = DefaultStorageHeader().WriteTo(data)
	assert.Nil(t, err)
	nvm.Write(align(data.Bytes()))
	assert.Nil(t, err)

	nvm.Sync()
	nvm.Close()
	//
	nvm, _, err = Open("foo-dio.lusf", true)
	defer os.Remove("foo-dio.lusf")
	assert.Nil(t, err)
	flag, err := fcntl(int(nvm.file.Fd()), syscall.F_GETFL, 0)
	assert.Nil(t, err)
	assert.Equal(t, true, isDirectIO(flag))
	nvm.Close()
}

func TestFileNVMEXLock(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-dio.lusf", 1024, true)
	assert.Nil(t, err)
	defer os.Remove("foo-dio.lusf")

	data := new(bytes.Buffer)
	err = DefaultStorageHeader().WriteTo(data)
	assert.Nil(t, err)
	nvm.Write(align(data.Bytes()))

	nvm.Sync()
	nvm.Close()

	nvm, _, err = Open("foo-dio.lusf", true)
	assert.Nil(t, err)

	flag, err := fcntl(int(nvm.file.Fd()), syscall.F_GETFL, 0)
	assert.Nil(t, err)
	assert.Equal(t, true, isExclusiveLock("foo-dio.lusf", flag))

	nvm.Close()

}

func TestFileReadHole1(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-dio.lusf", 33<<20, true)
	assert.Nil(t, err)
	defer os.Remove("foo-dio.lusf")
	nvm.Seek(32<<20, io.SeekStart)
	buf := alignedWithSize(32 << 20)
	//n, err := nvm.Read(buf)
	n, err := io.ReadFull(nvm, buf)
	assert.Equal(t, io.ErrUnexpectedEOF, err)
	assert.Equal(t, 1048576, n)
}

func TestFileReadHole2(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-dio.lusf", 33<<20, true)
	assert.Nil(t, err)
	defer os.Remove("foo-dio.lusf")
	nvm.Seek(0, io.SeekStart)
	buf := alignedWithSize(32 << 20)
	//n, err := nvm.Read(buf)
	n, err := io.ReadFull(nvm, buf)
	assert.Nil(t, err)
	assert.Equal(t, 32<<20, n)
}

func TestFileReadAtWriteAt(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-dio.lusf", 33<<20, true)
	assert.Nil(t, err)
	defer os.Remove("foo-dio.lusf")
	wbuf := alignedWithSize(512)
	wbuf[0] = 10
	wbuf[34] = 9
	wbuf[500] = 8
	rbuf := alignedWithSize(512)
	_, err = nvm.WriteAt(wbuf, 512)
	assert.Nil(t, err)
	_, err = nvm.ReadAt(rbuf, 512)
	assert.Nil(t, err)
	assert.Equal(t, wbuf, rbuf)
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

func fillBuf(buf []byte, x byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = x
	}
}
