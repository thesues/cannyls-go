package storage

import (
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/lumpindex"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/storage/allocator"
	"github.com/thesues/cannyls-go/storage/journal"
)

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
	alloc := allocator.New()
	alloc.RestoreFromIndex(file.BlockSize(), header.DataRegionSize, index.DataPortions())
	dataRegion := NewDataRegion(alloc, dataNVM)

	return &Storage{
		storageHeader: header,
		dataRegion:    dataRegion,
		journalRegion: journalRegion,
		index:         index,
	}, nil

}

func CreateCannylsStorage(path string, capacity uint64, journal_ratio float64) (*Storage, error) {
	file, err := nvm.CreateIfAbsent(path, capacity)
	if err != nil {
		return nil, err
	}
	header := makeHeader(file, 0.01)

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
