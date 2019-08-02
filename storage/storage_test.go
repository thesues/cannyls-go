package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
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


	storage.Close()

	//reopen the storage
	storage, err = OpenCannylsStorage("tmp11.lusf")
	assert.Nil(t, err)
	assert.Equal(t, []lump.LumpId{lumpid("00"), lumpid("11"), lumpid("22")}, storage.List())
	storage.Close()
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

		if _,_, err = storage.Delete(lumpidnum(i)); err != nil {
			fmt.Printf("%+v", err)
			break

		}
	}
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
