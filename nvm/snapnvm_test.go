package nvm

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/util"
)

func TestBackingFileCreate(t *testing.T) {
	f, err := CreateBackingFile("test", 10<<20, 10<<20)
	assert.Nil(t, err)
	defer os.Remove(f.fileName)
}

func TestBackingFileOpen(t *testing.T) {
	f, err := CreateBackingFile("test", 10<<20, 10<<20)
	defer os.Remove(f.fileName)
	assert.Nil(t, err)
	fileName := f.fileName
	buf := make([]byte, f.RegionSize())
	regionSize := f.RegionSize()
	buf[regionSize-1] = 'a'

	//1M(*) 2M 3M(*) 4M 5M 6M(*)
	f.WriteOffset(buf[:], 0)
	f.WriteOffset(buf[:], 2)
	f.Close()

	f, err = OpenBackingFile(fileName)
	assert.Equal(t, uint64(512+12*2), f.JournalEnd)
	assert.Equal(t, f.dataStart+(regionSize)*2, f.dataEnd)

	onBacking, onOrigin := f.GetCopyOffset((regionSize)-100, (regionSize)+100)
	assert.Equal(t, uint64(1024), f.dataStart)
	assert.Equal(t, 1, len(onBacking))
	assert.Equal(t, 1, len(onOrigin))
	assert.Equal(t, uint32(0), onBacking[0])
	assert.Equal(t, uint32(1), onOrigin[0])

	onBacking, onOrigin = f.GetCopyOffset((regionSize), (regionSize)*2)
	assert.Equal(t, 1, len(onOrigin))
	assert.Equal(t, 0, len(onBacking))
	assert.Equal(t, uint32(1), onOrigin[0])

	assert.Nil(t, err)
	fmt.Printf("%+v\n", f)
}

func TestOpenSnapFile(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-test.lusf", (32<<20)*10+(4<<10)) //10M + 4k
	assert.Nil(t, err)
	defer os.Remove("foo-test.lusf")

	//write header
	data := new(bytes.Buffer)
	err = DefaultStorageHeader().WriteTo(data)
	assert.Nil(t, err)
	nvm.Write(align(data.Bytes()))
	assert.Nil(t, err)
	nvm.Sync()

	snap_nvm, err := NewSnapshotNVM(nvm)
	assert.Nil(t, err)

	buf := alignedWithSize(1 << 20)
	buf[0] = 'a'
	snap_nvm.Seek(512, io.SeekStart)
	snap_nvm.Write(buf)
	snap_nvm.Close()

	//reopen
	nvm, _, err = Open("foo-test.lusf")
	assert.Nil(t, err)
	snap_nvm, err = NewSnapshotNVM(nvm)
	assert.Nil(t, err)
	buf[0] = 0 //clear
	snap_nvm.Seek(512, io.SeekStart)
	_, err = snap_nvm.Read(buf)
	assert.Nil(t, err)
	assert.Equal(t, byte('a'), buf[0])

}

func TestSimpleWriteSnapFile(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-test.lusf", (32<<20)*10+(4<<10)) //10M + 4k
	assert.Nil(t, err)
	defer os.Remove("foo-test.lusf")
	snap_nvm, err := NewSnapshotNVM(nvm)
	assert.Nil(t, err)

	err = snap_nvm.createSnapshotIfNeeded()
	assert.Nil(t, err)
	defer os.Remove(snap_nvm.myBackfile.GetFileName())

	//write
	wbuf := alignedWithSize(1 << 20)
	wbuf[0] = 'a'
	wbuf[1] = 'b'
	_, err = snap_nvm.Write(wbuf)
	assert.Nil(t, err)
	snap_nvm.Close()
	return

	//normal read
	rbuf := alignedWithSize(2)
	snap_nvm.Seek(0, io.SeekStart)
	n, err := snap_nvm.Read(rbuf)
	assert.Nil(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, byte('a'), rbuf[0])
	assert.Equal(t, byte('b'), rbuf[1])

	//snapshot read
	snapshotReader, err := snap_nvm.GetSnapshotReader()
	assert.Nil(t, err)
	n, err = snapshotReader.Read(rbuf)
	assert.Nil(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, byte(0), rbuf[0])
	assert.Equal(t, byte(0), rbuf[1])

	//write
	snap_nvm.Seek((32<<20)*2, io.SeekStart)
	snap_nvm.Write(wbuf)

	//snapshot read
	snapshotReader.Seek((32<<20)*2, io.SeekStart)
	n, err = snapshotReader.Read(rbuf)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, byte(0), rbuf[0])
	assert.Equal(t, byte(0), rbuf[1])
}

func TestOverWriteSnapFile(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-test.lusf", (32<<20)*10+(4<<10)) //10M + 4k
	assert.Nil(t, err)
	defer os.Remove("foo-test.lusf")

	snap_nvm, err := NewSnapshotNVM(nvm)
	//write 512 * 'a' at (32<<20) - 100
	var offset int64 = (32 << 20) - 512
	wdata := arrayWithValueSize(1024, 97)
	_, err = snap_nvm.Seek(offset, io.SeekStart)
	assert.Nil(t, err)
	n, err := snap_nvm.Write(align(wdata))
	assert.Nil(t, err)
	assert.Equal(t, 1024, n)

	snapshotReader, err := snap_nvm.GetSnapshotReader()
	assert.Nil(t, err)
	defer os.Remove(snap_nvm.myBackfile.GetFileName())
	var rdata [1024]byte
	_, err = snapshotReader.Seek(offset, io.SeekStart)
	assert.Nil(t, err)

	//partial read
	n, err = snapshotReader.Read(rdata[:])
	assert.Nil(t, err)
	assert.Equal(t, 512, n)
	assert.Equal(t, arrayWithValueSize(512, 97), rdata[0:512])

	/*
		//full read
		snapshotReader.Seek(offset, io.SeekStart)
		n, err = io.ReadFull(snapshotReader, rdata[:])
		assert.Nil(t, err)
		assert.Equal(t, 1024, n)
		assert.Equal(t, arrayWithValueSize(1024, 97), rdata[:])

		//overwrite
		offset = (32 << 20)
		wdata = arrayWithValueSize(512, 122)
		_, err = snap_nvm.Seek(offset, io.SeekStart)
		assert.Nil(t, err)
		n, err = snap_nvm.Write(align(wdata))
		assert.Nil(t, err)
		assert.Equal(t, 512, n)

		//read snapshot
		snapshotReader.Seek(offset, io.SeekStart)
		n, err = io.ReadFull(snapshotReader, rdata[0:100])
		assert.Nil(t, err)
		assert.Equal(t, 100, n)
		assert.Equal(t, arrayWithValueSize(100, 97), rdata[0:100])
	*/
	snap_nvm.Close()

}

func TestOverWriteSnapFileReOpen(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-test.lusf", (32<<20)*10+(4<<10)) //10M + 4k
	assert.Nil(t, err)
	defer os.Remove("foo-test.lusf")

	//write header
	data := new(bytes.Buffer)
	err = DefaultStorageHeader().WriteTo(data)
	assert.Nil(t, err)
	nvm.Write(align(data.Bytes()))
	assert.Nil(t, err)
	nvm.Sync()
	nvm.Close()

	nvm, header, err := Open("foo-test.lusf")
	snapFile, err := NewSnapshotNVM(nvm)
	assert.Nil(t, err)
	journalNVM, dataNVM := header.SplitRegion(snapFile)

	buf1 := arrayWithValueSize(512, 99)
	buf2 := arrayWithValueSize(512, 100)

	journalNVM.Write(align(buf1))
	dataNVM.Write(align(buf2))

	snapFile.createSnapshotIfNeeded()
	defer os.Remove(snapFile.myBackfile.GetFileName())

	snapFile.Close()
	//setup is done

	nvm, header, err = Open("foo-test.lusf")
	assert.Nil(t, err)
	snap_nvm, err := NewSnapshotNVM(nvm)

	if snap_nvm.myBackfile == nil {
		panic("testing: you have to open the backing files")
	}
	assert.Nil(t, err)

	journalNVM, dataNVM = header.SplitRegion(snap_nvm)

	buf3 := arrayWithValueSize(512, 101)
	fmt.Printf("splited journalNVM's Capacity is %d\n", journalNVM.Capacity())
	journalNVM.Write(align(buf3))
	dataNVM.Write(align(buf3))
	//???????????
	snap_nvm.Sync()
	fmt.Printf("raw size is %d\n", snap_nvm.RawSize())

	//FIXME
	snapshotReader, err := snap_nvm.GetSnapshotReader()
	assert.Nil(t, err)
	//header : 512
	//journalRegionSize
	//dataNVM
	var rdata [512]byte
	_, err = snapshotReader.Seek(512, io.SeekStart)
	assert.Nil(t, err)
	n, err := snapshotReader.Read(rdata[:])
	assert.Equal(t, 512, n)
	assert.Equal(t, buf1[:], rdata[:])

	_, err = snapshotReader.Seek(int64(512+header.JournalRegionSize), io.SeekStart)
	assert.Nil(t, err)
	n, err = snapshotReader.Read(rdata[:])
	assert.Equal(t, 512, n)
	assert.Equal(t, buf2[:], rdata[:])

}

func TestSnapfileBackup(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-test.lusf", (32<<20)*10+(4<<10)) //10M + 4k
	assert.Nil(t, err)
	defer os.Remove("foo-test.lusf")

	snapFile, err := NewSnapshotNVM(nvm)
	assert.Nil(t, err)

	buf := alignedWithSize(4 << 10)

	for i := 0; i < 11; i++ {
		fillBuf(buf, byte('a')+byte(i))
		_, err := snapFile.Seek(int64(i*(32<<20)), io.SeekStart)
		assert.Nil(t, err)
		n, err := snapFile.Write(buf)
		assert.Equal(t, (4 << 10), n)
		assert.Nil(t, err)
	}

	backupReader, err := snapFile.GetSnapshotReader()
	assert.Nil(t, err)
	defer os.Remove(snapFile.myBackfile.GetFileName())

	for i := 10; i >= 0; i -= 2 {
		fillBuf(buf, byte('a')+byte(i))
		_, err := snapFile.Seek(int64(i*(32<<20)), io.SeekStart)
		assert.Nil(t, err)
		_, err = snapFile.Write(buf)
		assert.Nil(t, err)
	}
	/*
		originFile: "abcdefghijk"
		newfile     "kbidgfehcja"
	*/

	rbuf := alignedWithSize(4 << 10)
	//read from current file
	snapFile.Seek((32 << 20), io.SeekStart)
	n, err := snapFile.Read(rbuf)
	assert.Equal(t, 4096, n)
	assert.Equal(t, byte('b'), rbuf[0])

	//snap read from backing file
	backupReader.Seek((32<<20)*10+10, io.SeekStart)
	n, err = backupReader.Read(rbuf[:])
	assert.Nil(t, err)
	assert.Equal(t, 4086, n)
	n, err = backupReader.Read(rbuf[:])
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, byte('k'), rbuf[0])

	backupReader.Seek(0, io.SeekStart)
	n, err = backupReader.Read(rbuf[:4])
	assert.Nil(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, byte('a'), rbuf[0])

	//snap read from origin file
	backupReader.Seek((32<<20)*9+1024, io.SeekStart)
	n, err = backupReader.Read(rbuf[:])
	assert.Equal(t, 4096, n)
	assert.Nil(t, err)
	assert.Equal(t, byte('j'), rbuf[0])

	backfile, err := os.OpenFile("backup.file", os.O_CREATE|os.O_RDWR, 0644)
	assert.Nil(t, err)
	defer os.Remove("backup.file")
	backupReader.Seek(0, io.SeekStart)
	io.Copy(backfile, backupReader)
	err = exec.Command("cmp", "-s", "backup.file", "foo-test.lusf").Run()
	assert.Nil(t, err)
}

func TestSnapDelete(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-test.lusf", (32<<20)*10+(4<<10)) //10M + 4k
	assert.Nil(t, err)
	defer os.Remove("foo-test.lusf")

	snapNVM, err := NewSnapshotNVM(nvm)
	assert.Nil(t, err)

	_, err = snapNVM.GetSnapshotReader()
	assert.Nil(t, err)
	defer snapNVM.DeleteSnapshot()
}

func TestSnapNVMReadAtWriteAt(t *testing.T) {
	file, err := CreateIfAbsent("foo-dio.lusf", 33<<20)
	assert.Nil(t, err)
	defer os.Remove("foo-dio.lusf")

	nvm, err := NewSnapshotNVM(file)
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

func TestSnapThreadSafe(t *testing.T) {
	nvm, err := CreateIfAbsent("foo-test.lusf", (32<<20)*10+(4<<10)) //10M + 4k
	assert.Nil(t, err)
	defer os.Remove("foo-test.lusf")

	snapNVM, err := NewSnapshotNVM(nvm)
	assert.Nil(t, err)

	stopper := util.NewStopper()
	buf := arrayWithValueSize(512*10, 101)
	_, err = snapNVM.Write(buf)
	assert.Nil(t, err)

	snapReader, err := snapNVM.GetSnapshotReader()
	assert.Nil(t, err)
	defer snapNVM.DeleteSnapshot()

	//1 write thread
	stopper.RunWorker(func() {
		for i := 0; i < 20; i++ {
			_, err := snapNVM.Write(buf)
			assert.Nil(t, err)
			time.Sleep(30 * time.Millisecond)
		}
	})
	//8 read thread
	foo := func() {
		for i := 0; i < 10; i++ {
			rbuf := alignedWithSize(512)
			_, err := snapNVM.ReadAt(rbuf, int64(i*512))
			assert.Nil(t, err)
			time.Sleep(20 * time.Millisecond)
		}
	}
	for i := 0; i < 8; i++ {
		stopper.RunWorker(func() {
			foo()
		})
	}
	//1 snapshot read
	snapBuf := make([]byte, 31)
	for {
		n, err := snapReader.Read(snapBuf)
		if err != nil && err != io.EOF {
			panic(err.Error())
		}
		assert.Equal(t, arrayWithValueSize(n, 101), snapBuf[:n])
		if err != nil {
			break
		}
	}
	stopper.Wait()
}
