package storage

import (
	"bytes"
	"fmt"

	"time"

	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/lumpindex"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/portion"
	"github.com/thesues/cannyls-go/storage/allocator"
	"github.com/thesues/cannyls-go/storage/journal"
)

var _ = fmt.Println
var (
	MAGIC_NUMBER = [4]byte{'l', 'u', 's', 'f'}
)

const (
	MAJOR_VERSION           uint16 = 1
	MINOR_VERSION           uint16 = 1
	MAX_JOURNAL_REGION_SIZE uint64 = (1 << 40) - 1
	MAX_DATA_REGION_SIZE    uint64 = MAX_JOURNAL_REGION_SIZE * uint64(block.MIN)
)

type Storage struct {
	storageHeader *nvm.StorageHeader
	dataRegion    *DataRegion
	journalRegion *journal.JournalRegion
	index         *lumpindex.LumpIndex
	innerNVM      nvm.NonVolatileMemory
	alloc         allocator.DataPortionAlloc
}

type StorageUsage struct {
	JournalCapacity uint64 `json:"jouranlcapacity"`
	DataCapacity    uint64 `json:"datacapacity"`
	FileCounts      uint64 `json:"filecounts"`
	MinIndex        int64  `json:"minindex"`
	MaxIndex        int64  `json:"maxindex"`
	FreeBytes       uint64 `json:"freebytes"`
	CurrentFileSize uint64 `json:"currentfilesize"`
}

func OpenCannylsStorage(path string) (*Storage, error) {
	file, header, err := nvm.Open(path)
	if err != nil {
		return nil, err
	}

	index := lumpindex.NewIndex()
	journalNVM, dataNVM := header.SplitRegion(file)

	journalRegion, err := journal.OpenJournalRegion(journalNVM)
	if err != nil {
		return nil, err
	}

	fmt.Printf("%v Start to restore index\n", time.Now())
	journalRegion.RestoreIndex(index)
	fmt.Printf("%v End to restore index\n", time.Now())
	fmt.Printf("Index's mem is %d\n", index.MemoryUsed())
	id, _ := index.Min()
	fmt.Printf("Min index is %d\n", id.U64())
	id, _ = index.Max()
	fmt.Printf("Max index is %d\n", id.U64())

	//use JudyAlloc as default
	alloc := allocator.NewJudyAlloc()
	fmt.Printf("%v :Start to restore allocator\n", time.Now())

	//  use RestoreFromIndex as default
	alloc.RestoreFromIndex(file.BlockSize(), header.DataRegionSize, index.DataPortions())
	/*
		alloc.RestoreFromIndexWithJudy(file.BlockSize(), header.DataRegionSize, index.JudyDataPortions())

		RestoreFromIndexWithJudy is 10% slower than RestoreFromIndex, But it takes significant less
			memory.
	*/

	fmt.Printf("%v :End to restore allocator\n", time.Now())
	dataRegion := NewDataRegion(alloc, dataNVM)

	return &Storage{
		storageHeader: header,
		dataRegion:    dataRegion,
		journalRegion: journalRegion,
		index:         index,
		innerNVM:      file,
		alloc:         alloc,
	}, nil

}

func CreateCannylsStorage(path string, capacity uint64, journal_ratio float64) (*Storage, error) {

	file, err := nvm.CreateIfAbsent(path, capacity)
	if err != nil {
		return nil, err
	}

	headBuf := new(bytes.Buffer)
	header := makeHeader(file, journal_ratio)

	if err = header.WriteHeaderRegionTo(headBuf); err != nil {
		return nil, err
	}
	//now headBuf's len should be at least 512

	journal.InitialJournalRegion(headBuf, file.BlockSize())
	//headbuf should be header(512) + (journal header)512 + (journal)512

	alignedBufHead := block.FromBytes(headBuf.Bytes(), file.BlockSize())
	alignedBufHead.Align()
	file.Write(alignedBufHead.AsBytes())

	if err = file.Sync(); err != nil {
		return nil, err
	}
	file.Close()

	return OpenCannylsStorage(path)
}

func makeHeader(file nvm.NonVolatileMemory, journal_ratio float64) nvm.StorageHeader {

	//total size
	totalSize := file.Capacity()
	headerSize := file.BlockSize().CeilAlign(uint64(nvm.FULL_HEADER_SIZE))

	//check capacity
	if totalSize < headerSize+uint64(file.BlockSize().AsU16()*3) {
		panic("file size is too small")
	}

	//check journal size
	tmp := float64(file.Capacity()) * journal_ratio
	journalSize := file.BlockSize().CeilAlign(uint64(tmp))
	if journalSize > MAX_JOURNAL_REGION_SIZE {
		panic("journal size is too big")
	}

	if journalSize < uint64(file.BlockSize().AsU16()*2) {
		journalSize = uint64(file.BlockSize().AsU16() * 2)
	}

	dataSize := totalSize - journalSize - headerSize
	dataSize = file.BlockSize().FloorAlign(dataSize)
	if dataSize > MAX_DATA_REGION_SIZE {
		panic(fmt.Sprintf("data size is too big: %d", dataSize))
	}

	if dataSize < uint64(file.BlockSize().AsU16()) {
		dataSize = uint64(file.BlockSize().AsU16())
	}

	header := nvm.DefaultStorageHeader()
	header.BlockSize = file.BlockSize()
	header.JournalRegionSize = journalSize
	header.DataRegionSize = dataSize
	return *header
}

func (store *Storage) Header() nvm.StorageHeader {
	return *store.storageHeader
}

func (store *Storage) SetAutomaticGcMode(gc bool) {
	store.journalRegion.SetAutomaticGcMode(gc)
}

func (store *Storage) List() []lump.LumpId {
	return store.index.List()
}

func (store *Storage) Usage() StorageUsage {

	var min, max int64

	if id, have := store.index.Min(); have {
		min = int64(id.U64())
	} else {
		min = -1
	}

	if id, have := store.index.Max(); have {
		max = int64(id.U64())
	} else {
		max = -1
	}

	return StorageUsage{
		JournalCapacity: store.Header().JournalRegionSize,
		DataCapacity:    store.Header().DataRegionSize,
		FileCounts:      store.index.Count(),
		MinIndex:        min,
		MaxIndex:        max,
		FreeBytes:       store.alloc.FreeCount() * uint64(store.Header().BlockSize.AsU16()),
		CurrentFileSize: uint64(store.innerNVM.RawSize()),
	}
}

func (store *Storage) MinId() (lump.LumpId, bool) {
	return store.index.Min()
}

func (store *Storage) MaxId() (lump.LumpId, bool) {
	return store.index.Max()
}

func (store *Storage) GenerateEmptyId() (id lump.LumpId, have bool) {
	id, have = store.MaxId()
	if have == false {
		//the store is empty, use 0 as the first id
		id = lump.FromU64(0, 0)
		have = true
		return
	} else {
		//if the ID is max, fallback to the front to find a new ID
		if id.IsMax() {
			id, have = store.index.FirstEmpty()
			return
		} else {
			id = id.Inc()
			return
		}
	}
}

func (store *Storage) JournalGC() {
	store.journalRegion.GcAllEntries(store.index)
}

type JournalSnapshot struct {
	UnreleasedHead uint64
	Head           uint64
	Tail           uint64
	Entries        []journal.JournalEntry
}

func (store *Storage) JournalSnapshot() JournalSnapshot {
	unreleasedhead, head, tail, entries := store.journalRegion.JournalEntries()
	return JournalSnapshot{
		UnreleasedHead: unreleasedhead,
		Head:           head,
		Tail:           tail,
		Entries:        entries,
	}
}

func (store *Storage) ListRange(start, end lump.LumpId) []lump.LumpId {
	return store.index.ListRange(start, end)
}

// Note the returned size is not accurate size of object, but aligned to block size.
// For accurate object size, use GetSize, which requires a disk IO.
func (store *Storage) GetSizeOnDisk(lumpid lump.LumpId) (size uint32, err error) {
	p, err := store.index.Get(lumpid)
	if err != nil {
		return 0, err
	}
	return p.SizeOnDisk(block.Min()), nil
}

// Get accurate size of object, require a disk IO
func (store *Storage) GetSize(lumpid lump.LumpId) (size uint32, err error) {
	p, err := store.index.Get(lumpid)
	if err != nil {
		return 0, err
	}
	switch v := p.(type) {
	case portion.DataPortion:
		return store.dataRegion.GetSize(v)
	case portion.JournalPortion:
		data, err := store.journalRegion.GetEmbededData(v)
		if err != nil {
			return 0, err
		}
		return uint32(len(data)), nil
	default:
		panic("never here")
	}
}

func (store *Storage) Get(lumpid lump.LumpId) ([]byte, error) {
	p, err := store.index.Get(lumpid)
	if err != nil {
		return nil, err
	}
	switch v := p.(type) {
	case portion.DataPortion:
		lumpdata, err := store.dataRegion.Get(v)
		if err != nil {
			return nil, err
		}
		return lumpdata.AsBytes(), nil
	case portion.JournalPortion:
		data, err := store.journalRegion.GetEmbededData(v)
		if err != nil {
			return nil, err
		}
		return data, nil
	default:
		panic("never here")
	}
}

func (store *Storage) GetWithOffset(lumpId lump.LumpId, startOffset uint32, length uint32) ([]byte, error) {
	p, err := store.index.Get(lumpId)
	if err != nil {
		return nil, err
	}
	switch v := p.(type) {
	case portion.DataPortion:
		return store.dataRegion.GetWithOffset(v, startOffset, length)
	case portion.JournalPortion:
		data, err := store.journalRegion.GetEmbededData(v)
		if err != nil {
			return nil, err
		}
		return data[startOffset : startOffset+length], nil
	default:
		panic("never here")
	}

}

func (store *Storage) Put(lumpid lump.LumpId, lumpdata lump.LumpData) (updated bool, err error) {

	err = nil
	if updated, _, err = store.deleteIfExist(lumpid, false); err != nil {
		return updated, err
	}

	dataPortion, err := store.dataRegion.Put(lumpdata)
	if err != nil {
		return
	}
	if err = store.journalRegion.RecordPut(store.index, lumpid, dataPortion); err != nil {
		//revert the dataPortion
		store.dataRegion.Release(dataPortion)
		return
	}

	store.index.InsertDataPortion(lumpid, dataPortion)
	return
}

func (store *Storage) PutEmbed(lumpid lump.LumpId, data []byte) (updated bool, err error) {
	if updated, _, err = store.deleteIfExist(lumpid, false); err != nil {
		return
	}
	err = store.journalRegion.RecordEmbed(store.index, lumpid, data)
	return
}

func (store *Storage) Delete(lumpid lump.LumpId) (updated bool, size uint32, err error) {
	updated, size, err = store.deleteIfExist(lumpid, true)
	return
}

func (store *Storage) deleteIfExist(lumpid lump.LumpId, doRecord bool) (bool, uint32, error) {
	p, err := store.index.Get(lumpid)

	//if not exist
	if err != nil {
		return false, 0, nil
	}

	//Because previous Get is ok, this Delete will surely success
	if ok := store.index.Delete(lumpid); ok == false {
		panic("Delete after Get failed, something bad happend")
	}

	if doRecord {
		store.journalRegion.RecordDelete(store.index, lumpid)
	}

	var releasedSize uint32

	switch v := p.(type) {
	case portion.DataPortion:
		releasedSize = uint32(v.Len) * uint32(store.innerNVM.BlockSize().AsU16())
		store.dataRegion.Release(v)
	case portion.JournalPortion:
		releasedSize = uint32(v.Len)

	}

	/*
		switch v := p.(type) {
		case portion.JournalPortion:
			if doRecord {
				return true, store.journalRegion.RecordDelete(store.index, lumpid)
			}
		case portion.DataPortion:
			store.dataRegion.Release(v)
		}
	*/
	return true, releasedSize, nil
}

/*
return value: len([]float) no more than 12800 points, each point is 4MB
*/
func (store *Storage) GetAllocationStatus() []float64 {
	//each point represents 4M bytes
	blockSizeBytes := store.storageHeader.BlockSize.AsU32()

	n := uint64((4 << 20) / blockSizeBytes)

	//if blockSizeBytes is bigger than 4M, return nil
	if n == 0 {
		return nil
	}

	total := store.storageHeader.DataRegionSize / uint64(blockSizeBytes)
	if total/n > 12800 {
		total = 12800 * n //max size is 50GB
	}

	return store.alloc.GetAllocationBitStatus(n, total)
}

func (store *Storage) JournalSync() {
	store.journalRegion.Sync()
}

func (store *Storage) Close() {
	store.journalRegion.Sync()
	store.innerNVM.Close()
}

func (store *Storage) RunSideJobOnce() {
	store.journalRegion.RunSideJobOnce(store.index)
}
