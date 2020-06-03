package storage

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	x "github.com/thesues/cannyls-go/metrics"
	"github.com/thesues/cannyls-go/storage/journal"
)

var _ = fmt.Print

func TestCreateCannylsStorageCreateOpen(t *testing.T) {
	//10M
	_, err := CreateCannylsStorage("test.lusf", 10<<20, 0.01)
	defer os.Remove("test.lusf")
	assert.Nil(t, err)
	assert.FileExists(t, "test.lusf")
}

func TestCreateCannylsStorageDeleteReturnSize(t *testing.T) {

	storage, err := CreateCannylsStorage("tmp11.lusf", 10<<20, 0.01)
	storage.Put(lumpid("0000"), zeroedData(512*3+10))

	updated, size, err := storage.Delete(lumpid("0000"))

	assert.Nil(t, err)
	assert.True(t, updated)
	assert.Equal(t, uint32(512*4), size)

	defer storage.Close()
	defer os.Remove("tmp11.lusf")
}

func TestStorage_GetSize(t *testing.T) {
	storage, err := CreateCannylsStorage("tmpsize.lusf", 10<<20, 0.01)
	assert.Nil(t, err)
	defer storage.Close()
	defer os.Remove("tmpsize.lusf")
	_, err = storage.Put(lumpid("0000"), zeroedData(512*3+10))
	assert.Nil(t, err)

	sizeOnDisk, err := storage.GetSizeOnDisk(lumpid("0000"))
	assert.Nil(t, err)
	assert.Equal(t, sizeOnDisk, uint32(2048))

	size, err := storage.GetSize(lumpid("0000"))
	assert.Nil(t, err)
	assert.Equal(t, size, uint32(1546))

	_, err = storage.GetSizeOnDisk(lumpid("1234"))
	assert.NotNil(t, err)
	_, err = storage.GetSize(lumpid("1234"))
	assert.NotNil(t, err)
	_, err = storage.Put(lumpid("1234"), zeroedData(512*2+233))
	assert.Nil(t, err)
	sizeOnDisk, err = storage.GetSizeOnDisk(lumpid("1234"))
	assert.Nil(t, err)
	assert.Equal(t, sizeOnDisk, uint32(1536))
	size, err = storage.GetSize(lumpid("1234"))
	assert.Nil(t, err)
	assert.Equal(t, size, uint32(1257))
}

func TestCreateCannylsStorageWork(t *testing.T) {
	//10M
	var size uint32
	storage, err := CreateCannylsStorage("tmp11.lusf", 10<<20, 0.01)
	defer os.Remove("tmp11.lusf")

	assert.Nil(t, err)
	assert.NotZero(t, storage)

	data, err := storage.Get(lumpid("01"))
	assert.Error(t, err)
	fmt.Println(data)

	updated, err := storage.PutEmbed(lumpid("00"), []byte("hello"))
	assert.Nil(t, err)
	assert.False(t, updated)

	//snapshot
	storage.JournalSync()
	reader, err := storage.GetSnapshotReader()
	defer storage.innerNVM.DeleteSnapshot()
	assert.Nil(t, err)

	updated, err = storage.PutEmbed(lumpid("00"), []byte("hello"))
	assert.Nil(t, err)
	assert.True(t, updated)

	data, err = storage.Get(lumpid("00"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("hello"), data)

	updated, size, err = storage.Delete(lumpid("00"))
	assert.Nil(t, err)
	assert.True(t, updated)
	assert.Equal(t, uint32(5), size)

	updated, size, err = storage.Delete(lumpid("00"))
	assert.Nil(t, err)
	assert.False(t, updated)
	assert.Equal(t, uint32(0), size)

	storage.PutEmbed(lumpid("00"), []byte("hello"))
	storage.PutEmbed(lumpid("11"), []byte("world"))

	for i := 0; i < 10; i++ {
		storage.RunSideJobOnce()
		_, err = storage.PutEmbed(lumpid("22"), []byte("quux"))
		assert.Nil(t, err)
		storage.Delete(lumpid("22"))
	}
	storage.PutEmbed(lumpid("22"), []byte("hello, world"))

	backup, _ := os.OpenFile("backup.lusf", os.O_CREATE|os.O_RDWR, 0644)
	defer os.Remove("backup.lusf")
	storage.JournalSync()
	io.Copy(backup, reader)

	storage.Close()

	//reopen the storage
	/*
		storage, err = OpenCannylsStorage("tmp11.lusf")
		assert.Nil(t, err)
		assert.Equal(t, []lump.LumpId{lumpid("00"), lumpid("11"), lumpid("22")}, storage.List())
		storage.Close()
	*/
}

func TestCreateCannylsStorageFull(t *testing.T) {
	storage, err := CreateCannylsStorage("tmp11.lusf", 1024*1024, 0.01)
	assert.Nil(t, err)
	defer os.Remove("tmp11.lusf")

	updated, err := storage.Put(lumpid("0000"), zeroedData(512*1024))
	assert.Nil(t, err)
	assert.False(t, updated)

	updated, err = storage.Put(lumpid("0000"), zeroedData(512*1024))
	assert.Nil(t, err)
	assert.True(t, updated)

	updated, err = storage.Put(lumpid("1111"), zeroedData(512*1024))
	assert.Error(t, err)

	storage.Delete(lumpid("0000"))
	updated, err = storage.Put(lumpid("1111"), zeroedData(512*1024))
	assert.Nil(t, err)

}

func TestCreateCannylsStorageFullGC(t *testing.T) {

	storage, err := CreateCannylsStorage("tmp11.lusf", 1024*1024, 0.01)
	assert.Nil(t, err)
	defer os.Remove("tmp11.lusf")

	storage.SetAutomaticGcMode(false)

	storage.Put(lumpid("0000"), zeroedData(42))
	storage.Put(lumpid("0010"), zeroedData(42))

	entries := storage.JournalSnapshot().Entries

	assert.Equal(t, 2, len(entries))
	assert.True(t, isPut(entries[0], lumpid("0000")))
	assert.True(t, isPut(entries[1], lumpid("0010")))

	storage.JournalGC()

	storage.Delete(lumpid("0000"))
	storage.Delete(lumpid("0010"))

	entries = storage.JournalSnapshot().Entries
	assert.Equal(t, 4, len(entries))
	//fmt.Printf("%+v\n", entries)
	assert.True(t, isPut(entries[0], lumpid("0000")))
	assert.True(t, isPut(entries[1], lumpid("0010")))
	assert.True(t, isDelete(entries[2], lumpid("0000")))
	assert.True(t, isDelete(entries[3], lumpid("0010")))

	storage.JournalGC()

	entries = storage.JournalSnapshot().Entries
	assert.Equal(t, 0, len(entries))

}

//FIXME
func TestCreateCannylsNoOverflow(t *testing.T) {
	storage, err := CreateCannylsStorage("tmp11.lusf", 400*1024, 0.01)
	assert.Nil(t, err)
	defer os.Remove("tmp11.lusf")

	storage.SetAutomaticGcMode(false)

	assert.Equal(t, uint64(4096), storage.storageHeader.JournalRegionSize)

	for i := 0; i < 60; i++ {
		storage.Put(lumpidnum(i), zeroedData(42))
	}
	for i := 0; i < 20; i++ {
		storage.Delete(lumpidnum(i))
	}

	// (5+8+2+5)*60 + (5+8) * 20 == 1460
	snapshot := storage.JournalSnapshot()
	assert.Equal(t, uint64(0), snapshot.UnreleasedHead)
	assert.Equal(t, uint64(0), snapshot.Head)
	assert.Equal(t, uint64(1460), snapshot.Tail)

	storage.JournalGC()

	// (60-20) * (5 + 8 + 2 + 5) + 2100 == 2260
	snapshot = storage.JournalSnapshot()
	assert.Equal(t, uint64(1460), snapshot.UnreleasedHead)
	assert.Equal(t, uint64(1460), snapshot.Head)
	assert.Equal(t, uint64(2260), snapshot.Tail)

	// 2260 + 40 * PUTRECORDSIZE
	storage.JournalGC()
	snapshot = storage.JournalSnapshot()
	assert.Equal(t, uint64(2260), snapshot.UnreleasedHead)
	assert.Equal(t, uint64(2260), snapshot.Head)
	assert.Equal(t, uint64(3060), snapshot.Tail)

}

func TestStorageLoopForEver1024(t *testing.T) {
	var err error
	storage, err := CreateCannylsStorage("tmp11.lusf", 1024*1024, 0.8)
	assert.Nil(t, err)
	//storage, err := CreateCannylsStorage("tmp11.lusf", 10*1024, 0.8) test case
	fmt.Printf("Journal Region Size is %d\n", storage.storageHeader.JournalRegionSize)
	defer os.Remove("tmp11.lusf")
	for i := 0; i < 50000; i++ {
		if _, err = storage.PutEmbed(lumpidnum(i), []byte("foo")); err != nil {
			fmt.Printf("%+v", err)
			break
		}

		if _, _, err = storage.Delete(lumpidnum(i)); err != nil {
			fmt.Printf("%+v", err)
			break

		}
	}
}

//slow
func TestStorageLoopForEver32(t *testing.T) {
	var err error
	storage, err := CreateCannylsStorage("tmp11.lusf", 32*1024, 0.8)
	assert.Nil(t, err)
	defer os.Remove("tmp11.lusf")
	for i := 0; i < 50000; i++ {
		if _, err = storage.PutEmbed(lumpidnum(i), []byte("foo")); err != nil {
			fmt.Printf("%+v", err)
			break
		}

		if _, _, err = storage.Delete(lumpidnum(i)); err != nil {
			fmt.Printf("%+v", err)
			break

		}
	}
}

func TestStorage_paddingWithZero(t *testing.T) {
	payload := []byte("abcdefghijklmn")

	result1 := paddingWithZero(payload, 3, 100)
	assert.Equal(t, result1,
		[]byte("\x00\x00\x00abcdefghijklmn\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))

	result2 := paddingWithZero(payload, 0, 20)
	assert.Equal(t, result2,
		[]byte("abcdefghijklmn\x00\x00\x00\x00\x00\x00"))

	result3 := paddingWithZero(payload, 2, 0)
	assert.Equal(t, result3,
		[]byte("\x00\x00abcdefghijklmn"))
}

func TestStorage_Offset(t *testing.T) {
	storage, err := CreateCannylsStorage("offset.lusf",
		10<<20, 0.1)
	assert.Nil(t, err)
	defer storage.Close()
	defer os.Remove("offset.lusf")

	piece1 := dataFromBytes([]byte("hehe"))
	err = storage.PutWithOffset(lumpidnum(1), piece1, 0, 20)
	assert.Nil(t, err)
	result, err := storage.Get(lumpidnum(1))
	assert.Nil(t, err)
	assert.Equal(t, result, []byte("hehe\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
	piece2 := dataFromBytes([]byte("haha"))
	err = storage.PutWithOffset(lumpidnum(1), piece2, 4, 0)
	assert.Nil(t, err)
	piece3 := dataFromBytes([]byte("2233"))
	err = storage.PutWithOffset(lumpidnum(1), piece3, 8, 0)
	assert.Nil(t, err)
	result, err = storage.Get(lumpidnum(1))
	assert.Nil(t, err)
	assert.Equal(t, result, []byte("hehehaha2233\x00\x00\x00\x00\x00\x00\x00\x00"))
	result, err = storage.GetWithOffset(lumpidnum(1), 6, 10)
	assert.Nil(t, err)
	assert.Equal(t, result, []byte("ha2233\x00\x00\x00\x00"))
	result, err = storage.GetWithOffset(lumpidnum(1), 20, 10)
	assert.Nil(t, err)
	assert.Equal(t, result, []byte(""))
	result, err = storage.GetWithOffset(lumpidnum(1), 100, 10)
	assert.NotNil(t, err)
	err = storage.PutWithOffset(lumpidnum(1), piece3, 30, 0)
	assert.NotNil(t, err)
	err = storage.PutWithOffset(lumpidnum(1), piece3, 300, 0)
	assert.NotNil(t, err)

	piece4 := make([]byte, 1000)
	for i := 0; i < 1000; i++ {
		piece4[i] = byte(i)
	}
	err = storage.PutWithOffset(lumpidnum(2), dataFromBytes(piece4),
		10, 1024)
	assert.Nil(t, err)
	err = storage.PutWithOffset(lumpidnum(2), piece1, 10, 0)
	assert.Nil(t, err)
	err = storage.PutWithOffset(lumpidnum(2), piece2, 1020, 0)
	assert.Nil(t, err)
	result, err = storage.GetWithOffset(lumpidnum(2), 0, 16)
	assert.Nil(t, err)
	assert.Equal(t, result, []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00hehe\x04\x05"))
	result, err = storage.GetWithOffset(lumpidnum(2), 1020, 4)
	assert.Nil(t, err)
	assert.Equal(t, result, []byte("haha"))
}

func BenchmarkStoragePutEmbeded(b *testing.B) {
	var err error
	storage, err := CreateCannylsStorage("bench.lusf", 1024*1024*1024, 0.9)
	if err != nil {
		panic("failed to create bench.lusf")
	}
	defer os.Remove("bench.lusf")
	data := []byte("foo")
	for i := 0; i < b.N; i++ {
		if _, err = storage.PutEmbed(lumpidnum(i), data); err != nil {
			fmt.Printf("ERR is %+v\n", err)
			break
		}
	}
}

func BenchmarkStoragePutData(b *testing.B) {
	storage, _ := CreateCannylsStorage("bench.lusf", 1024*1024*1024, 0.5)
	defer os.Remove("bench.lusf")
	for i := 0; i < b.N; i++ {
		d := zeroedData(42)
		storage.Put(lumpidnum(i), d)
		d.Inner.Resize(42)
	}
}

func lumpid(s string) lump.LumpId {
	l, err := lump.FromString(s)
	if err != nil {
		panic("failed to create lumpid")
	}
	return l
}

func lumpidnum(n int) lump.LumpId {
	l := lump.FromU64(0, uint64(n))
	return l
}

func zeroedData(size int) lump.LumpData {
	lumpData := lump.NewLumpDataAligned(size, block.Min())
	buf := lumpData.AsBytes()
	for i := 0; i < len(buf); i++ {
		buf[i] = 0
	}
	return lumpData
}

func dataFromBytes(payload []byte) lump.LumpData {
	data := lump.NewLumpDataAligned(len(payload), block.Min())
	copy(data.AsBytes(), payload)
	return data
}

func isPut(entry journal.JournalEntry, id lump.LumpId) bool {
	r, ok := entry.Record.(journal.PutRecord)
	if ok {
		return r.LumpID == id
	}
	return false
}

func isDelete(entry journal.JournalEntry, id lump.LumpId) bool {
	r, ok := entry.Record.(journal.DeleteRecord)
	if ok {
		return r.LumpID == id
	}
	return false
}

/*learn from this article:
*https://blog.questionable.services/article/testing-http-handlers-go/
 */
func TestMetricHttpSever(t *testing.T) {
	//500K
	storage, err := CreateCannylsStorage("tmp11.lusf", 500<<10, 0.5)
	defer os.Remove("tmp11.lusf")

	dummyData := make([]byte, 1024)

	for i := 0; i < 130; i++ {
		storage.PutEmbed(lumpidnum(i), dummyData)
	}

	storage.JournalSync()
	storage.JournalSync()

	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	x.PrometheusHandler.ServeHTTP(rr, req)
	fmt.Printf("%s", rr.Body.String())
}

func storageRangeDelete(t *testing.T, reOpen bool, isEmbeded bool) {
	var err error
	storage, err := CreateCannylsStorage("tmp11.lusf", 1024*1024, 0.8)
	assert.Nil(t, err)
	defer os.Remove("tmp11.lusf")
	for i := 0; i < 100; i++ {
		if isEmbeded {
			_, err = storage.PutEmbed(lumpidnum(i), []byte("foo"))
			assert.Nil(t, err)
		} else {
			lumpData := dataFromBytes([]byte("foo"))
			_, err = storage.Put(lumpidnum(i), lumpData)
			assert.Nil(t, err)

		}

	}

	storage.DeleteRange(lumpidnum(10), lumpidnum(10000))

	if reOpen {
		storage.Close()
		storage, err = OpenCannylsStorage("tmp11.lusf")
		assert.Nil(t, err)
	}

	for i := 0; i < 10; i++ {
		data, err := storage.Get(lumpidnum(i))
		assert.Nil(t, err)
		assert.Equal(t, []byte("foo"), data)
	}
	for i := 11; i < 100; i++ {
		_, err := storage.Get(lumpidnum(i))
		assert.Error(t, err)
	}
}

func TestStorageRangeDelete(t *testing.T) {
	storageRangeDelete(t, true, true)
	storageRangeDelete(t, true, false)
	storageRangeDelete(t, false, true)
	storageRangeDelete(t, false, false)
	storageRangeDelete(t, true, true)
	storageRangeDelete(t, true, true)

}
