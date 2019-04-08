package storage

import (
	"fmt"
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
}

func OpenCannylsStorage(path string) (*Storage, error) {
	file, header, err := nvm.Open(path)
	if err != nil {
		return nil, nil
	}

	index := lumpindex.NewIndex()
	journalNVM, dataNVM := header.SplitRegion(file)

	journalRegion, err := journal.OpenJournalRegion(journalNVM)
	if err != nil {
		return nil, err
	}

	journalRegion.RestoreIndex(index)
	alloc := allocator.New()
	alloc.RestoreFromIndex(file.BlockSize(), header.DataRegionSize, index.DataPortions())
	dataRegion := NewDataRegion(alloc, dataNVM)

	return &Storage{
		storageHeader: header,
		dataRegion:    dataRegion,
		journalRegion: journalRegion,
		index:         index,
		innerNVM:      file,
	}, nil

}

func CreateCannylsStorage(path string, capacity uint64, journal_ratio float64) (*Storage, error) {
	file, err := nvm.CreateIfAbsent(path, capacity)
	if err != nil {
		return nil, err
	}
	header := makeHeader(file, journal_ratio)

	header.WriteHeaderRegionTo(file)

	file.Sync()

	journal.InitialJournalRegion(file, file.BlockSize())

	file.Sync()
	file.Close()

	return OpenCannylsStorage(path)
}

func makeHeader(file nvm.NonVolatileMemory, journal_ratio float64) nvm.StorageHeader {

	//total size
	totalSize := file.Capacity()
	headerSize := file.BlockSize().CeilAlign(uint64(nvm.FULL_HEADER_SIZE))

	//check capacity
	if totalSize < headerSize {
		panic("file size is too small")
	}

	//check journal size
	tmp := float64(file.Capacity()) * journal_ratio
	journalSize := file.BlockSize().CeilAlign(uint64(tmp))
	if journalSize > MAX_JOURNAL_REGION_SIZE {
		panic("journal size is too big")
	}

	dataSize := totalSize - journalSize - headerSize
	dataSize = file.BlockSize().FloorAlign(dataSize)
	if dataSize > MAX_DATA_REGION_SIZE {
		panic("data size is too big")
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

func (store *Storage) Put(lumpid lump.LumpId, lumpdata lump.LumpData) (updated bool, err error) {

	err = nil
	if updated, err = store.deleteIfExist(lumpid, false); err != nil {
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
	if updated, err = store.deleteIfExist(lumpid, false); err != nil {
		return
	}
	err = store.journalRegion.RecordEmbed(store.index, lumpid, data)
	return
}

func (store *Storage) Delete(lumpid lump.LumpId) (updated bool, err error) {
	updated, err = store.deleteIfExist(lumpid, true)
	return
}

func (store *Storage) deleteIfExist(lumpid lump.LumpId, doRecord bool) (bool, error) {
	p := store.index.Delete(lumpid)
	if p == nil {
		return false, nil
	}

	if doRecord {
		store.journalRegion.RecordDelete(store.index, lumpid)
	}
	switch v := p.(type) {
	case portion.DataPortion:
		store.dataRegion.Release(v)
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
	return true, nil
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
