package storage

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/lumpindex"
	x "github.com/thesues/cannyls-go/metrics"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/portion"
	"github.com/thesues/cannyls-go/storage/allocator"
	"github.com/thesues/cannyls-go/storage/journal"
	"github.com/thesues/cannyls-go/util"
	ostats "go.opencensus.io/stats"
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
	i                     sync.RWMutex
	jr                    sync.Mutex //protect journal region(read/write)
	storageHeader         *nvm.StorageHeader
	dataRegion            *DataRegion
	journalRegion         *journal.JournalRegion
	index                 *lumpindex.LumpIndex
	innerNVM              *nvm.SnapNVM
	alloc                 allocator.DataPortionAlloc
	updateCapacityStopper *util.Stopper
	opened                bool
}

type StorageUsage struct {
	JournalCapacity   uint64 `json:"journalcapacity"`
	DataCapacity      uint64 `json:"datacapacity"`
	FileCounts        uint64 `json:"filecounts"`
	DataFreeBytes     uint64 `json:"datafreebytes"`
	JournalUsageBytes uint64 `json:"journalusagebytes"`
	MaxSegmentSize    uint64 `json:"maxsegmentsize"`
	//	CurrentFileSize uint64 `json:"currentfilesize"`
}

func OpenCannylsStorage(path string) (*Storage, error) {
	file, header, err := nvm.Open(path)
	if err != nil {
		return nil, err
	}
	snapNVM, err := nvm.NewSnapshotNVM(file)
	if err != nil {
		return nil, err
	}

	index := lumpindex.NewIndex()
	journalNVM, dataNVM := header.SplitRegion(snapNVM)

	journalRegion, err := journal.OpenJournalRegion(journalNVM)
	if err != nil {
		return nil, err
	}

	journalRegion.RestoreIndex(index)
	/*
		fmt.Printf("Index's mem is %d\n", index.MemoryUsed())
		id, _ := index.Min()
		fmt.Printf("Min index is %d\n", id.U64())
		id, _ = index.Max()
		fmt.Printf("Max index is %d\n", id.U64())
	*/

	//use JudyAlloc as default
	alloc := allocator.NewJudyAlloc()

	//  use RestoreFromIndex as default
	alloc.RestoreFromIndex(snapNVM.BlockSize(), header.DataRegionSize, index.DataPortions())
	/*
		alloc.RestoreFromIndexWithJudy(file.BlockSize(), header.DataRegionSize, index.JudyDataPortions())

		RestoreFromIndexWithJudy is 10% slower than RestoreFromIndex, But it takes significant less
			memory.
	*/

	dataRegion := NewDataRegion(alloc, dataNVM)

	//Add a go routing to collect capacity information into metric

	s := &Storage{
		storageHeader:         header,
		dataRegion:            dataRegion,
		journalRegion:         journalRegion,
		index:                 index,
		innerNVM:              snapNVM,
		alloc:                 alloc,
		updateCapacityStopper: util.NewStopper(),
		opened:                true,
	}

	//RunWorker == go func()
	s.updateCapacityStopper.RunWorker(func() {
		updateUsageInfo(s)
	})

	return s, nil
}

//go routine to collect capacity data
func updateUsageInfo(store *Storage) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	//on start, record the capacity first
	ctx := context.Background()
	ostats.Record(ctx, x.JournalRegionMetric.Capacity.M(int64(store.journalRegion.Usage())))
	for {
		select {
		case <-ticker.C:
			ctx := context.Background()
			ostats.Record(ctx, x.JournalRegionMetric.Capacity.M(int64(store.journalRegion.Usage())))
			//TODO, update data region usage in the future
		case <-store.updateCapacityStopper.ShouldStop():
			return
		}
	}

}

func CreateCannylsStorage(path string, capacity uint64, journal_ratio float64) (*Storage, error) {

	file, err := nvm.CreateIfAbsent(path, capacity)
	if err != nil {
		return nil, err
	}
	snapNVM, err := nvm.NewSnapshotNVM(file)
	if err != nil {
		return nil, err
	}

	headBuf := new(bytes.Buffer)
	header := makeHeader(snapNVM, journal_ratio)

	if err = header.WriteHeaderRegionTo(headBuf); err != nil {
		return nil, err
	}
	//now headBuf's len should be at least 512

	journal.InitialJournalRegion(headBuf, snapNVM.BlockSize())
	//headbuf should be header(512) + (journal header)512 + (journal)512

	alignedBufHead := block.FromBytes(headBuf.Bytes(), snapNVM.BlockSize())
	alignedBufHead.Align()
	snapNVM.Write(alignedBufHead.AsBytes())

	if err = snapNVM.Sync(); err != nil {
		return nil, err
	}
	snapNVM.Close()

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
	store.i.RLock()
	defer store.i.RUnlock()
	if !store.opened {
		return nil
	}
	return store.index.List()
}

func (store *Storage) CreateSnapshot() error {
	store.jr.Lock()
	defer store.jr.Unlock()
	if !store.opened {
		return internalerror.StorageClosed
	}
	store.journalSync()
	return store.innerNVM.CreateSnapshotIfNeeded()
}

func (store *Storage) DeleteSnapshot() error {
	store.jr.Lock()
	defer store.jr.Unlock()
	if !store.opened {
		return internalerror.StorageClosed
	}
	return store.innerNVM.DeleteSnapshot()
}

func (store *Storage) GetSnapshotReader() (*nvm.SnapshotReader, error) {
	store.jr.Lock()
	defer store.jr.Unlock()
	if !store.opened {
		return nil, internalerror.StorageClosed
	}
	store.journalSync()
	reader, err := store.innerNVM.GetSnapshotReader()
	if err != nil {
		return nil, errors.Errorf("failed to get Snapshot reader %+v", err)
	}
	return reader, err
}

func (store *Storage) Usage() StorageUsage {
	blockSize := uint64(store.Header().BlockSize.AsU16())
	return StorageUsage{
		JournalCapacity:   store.Header().JournalRegionSize,
		DataCapacity:      store.Header().DataRegionSize,
		FileCounts:        store.index.Count(),
		DataFreeBytes:     store.alloc.FreeCount() * blockSize,
		JournalUsageBytes: store.journalRegion.Usage(),
		MaxSegmentSize:    util.Min(lump.LUMP_MAX_SIZE, store.alloc.MaxSegmentSize()*blockSize-2),
		//	CurrentFileSize: uint64(store.innerNVM.RawSize()),
	}
}

func (store *Storage) MinId() (lump.LumpId, bool) {
	store.i.RLock()
	defer store.i.RUnlock()
	if !store.opened {
		return lump.EmptyLump(), false
	}
	return store.index.Min()
}

func (store *Storage) MaxId() (lump.LumpId, bool) {
	store.i.RLock()
	defer store.i.RUnlock()
	if !store.opened {
		return lump.EmptyLump(), false
	}
	return store.index.Max()
}

func (store *Storage) First(id lump.LumpId) (lump.LumpId, error) {
	store.i.RLock()
	defer store.i.RUnlock()
	if !store.opened {
		return lump.EmptyLump(), internalerror.StorageClosed
	}
	return store.index.First(id)
}

func (store *Storage) GenerateEmptyId() (id lump.LumpId, have bool) {
	store.i.RLock()
	defer store.i.RUnlock()
	if !store.opened {
		return lump.EmptyLump(), false
	}
	id, have = store.index.Max()
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
	store.i.Lock()
	defer store.i.Unlock()
	store.jr.Lock()
	defer store.jr.Unlock()
	if !store.opened {
		return
	}
	store.journalRegion.GcAllEntries(store.index)
}

type JournalSnapshot struct {
	UnreleasedHead uint64
	Head           uint64
	Tail           uint64
	Entries        []journal.JournalEntry
}

func (store *Storage) JournalSnapshot() JournalSnapshot {
	store.jr.Lock()
	defer store.jr.Unlock()
	unreleasedhead, head, tail, entries := store.journalRegion.JournalEntries()
	return JournalSnapshot{
		UnreleasedHead: unreleasedhead,
		Head:           head,
		Tail:           tail,
		Entries:        entries,
	}
}

func (store *Storage) ListRange(start, end lump.LumpId, maxSize uint64) []lump.LumpId {
	store.i.RLock()
	defer store.i.RUnlock()
	if !store.opened {
		return nil
	}
	return store.index.ListRange(start, end, maxSize)
}

// Note the returned size is not accurate size of object, but aligned to block size.
// For accurate object size, use GetSize, which requires a disk IO.
func (store *Storage) GetSizeOnDisk(lumpid lump.LumpId) (size uint32, err error) {
	store.i.RLock()
	defer store.i.RUnlock()
	if !store.opened {
		return 0, internalerror.StorageClosed
	}
	p, err := store.index.Get(lumpid)
	if err != nil {
		return 0, err
	}
	return p.SizeOnDisk(block.Min()), nil
}

// Get accurate size of object, require a disk IO
func (store *Storage) GetSize(lumpid lump.LumpId) (size uint32, err error) {
	store.i.RLock()
	if !store.opened {
		store.i.RUnlock()
		return 0, internalerror.StorageClosed
	}
	p, err := store.index.Get(lumpid)

	store.i.RUnlock()

	if err != nil {
		return 0, err
	}
	switch v := p.(type) {
	case portion.DataPortion:
		return store.dataRegion.GetSize(v)
	case portion.JournalPortion:
		store.jr.Lock()
		data, err := store.journalRegion.GetEmbededData(v)
		store.jr.Unlock()
		if err != nil {
			return 0, err
		}
		return uint32(len(data)), nil
	default:
		panic("never here")
	}
}

func (store *Storage) Get(lumpid lump.LumpId) ([]byte, error) {
	store.i.RLock()
	if !store.opened {
		store.i.RUnlock()
		return nil, internalerror.StorageClosed
	}
	p, err := store.index.Get(lumpid)
	store.i.RUnlock()
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
		store.jr.Lock()
		data, err := store.journalRegion.GetEmbededData(v)
		store.jr.Unlock()
		if err != nil {
			return nil, err
		}
		return data, nil
	default:
		panic("never here")
	}
}

func (store *Storage) GetWithOffset(lumpId lump.LumpId, startOffset uint32, length uint32) ([]byte, error) {
	store.i.RLock()
	if !store.opened {
		store.i.RUnlock()
		return nil, internalerror.StorageClosed
	}
	p, err := store.index.Get(lumpId)
	store.i.RUnlock()
	if err != nil {
		return nil, err
	}
	switch v := p.(type) {
	case portion.DataPortion:
		return store.dataRegion.GetWithOffset(v, startOffset, length)
	case portion.JournalPortion:
		store.jr.Lock()
		data, err := store.journalRegion.GetEmbededData(v)
		store.jr.Unlock()
		if err != nil {
			return nil, err
		}
		return data[startOffset : startOffset+length], nil
	default:
		panic("never here")
	}

}

func (store *Storage) put(lumpid lump.LumpId, lumpdata lump.LumpData) (err error) {
	dataPortion, err := store.dataRegion.Put(lumpdata)
	if err != nil {
		return
	}

	store.i.Lock()
	defer store.i.Unlock()

	store.jr.Lock()
	err = store.journalRegion.RecordPut(store.index, lumpid, dataPortion)
	store.jr.Unlock()

	if err != nil {
		// revert the dataPortion
		store.dataRegion.Release(dataPortion)
		return
	}

	store.index.InsertDataPortion(lumpid, dataPortion)

	if int64(lumpdata.Inner.Len()) == 1 {

		panic("FUCK")
	}
	return
}

func (store *Storage) Put(lumpid lump.LumpId, lumpdata lump.LumpData) (updated bool, err error) {
	err = nil
	if updated, _, err = store.deleteIfExist(lumpid, false); err != nil {
		return updated, err
	}
	return updated, store.put(lumpid, lumpdata)
}

// padding payload with necessary zeros before and after, return:
// [0: startOffset) + [startOffset, startOffset+len(payload)) + [startOffset+len(payload), reservation)
// prefix zero        payload                                   suffix zero
// total length is max(reservation, startOffset + len(payload))
func paddingWithZero(payload []byte, startOffset, reservation uint32) []byte {
	totalLength := reservation
	if startOffset+uint32(len(payload)) > reservation {
		totalLength = startOffset + uint32(len(payload))
	}
	padded := make([]byte, totalLength, totalLength)
	copy(padded[startOffset:], payload)
	return padded
}

// Put object with offset and space reservation.
// For object creation, object size is max(reservation, startOffset + len(lumpdata));
// for object update, `reservation` is ignored since space is already allocated,
// and return error when write offset exceeds reserved object size.
// Untouched space are zeroed.
func (store *Storage) PutWithOffset(lumpid lump.LumpId, lumpdata lump.LumpData,
	startOffset uint32, reservation uint32) (err error) {

	store.i.RLock()
	if !store.opened {
		store.i.RUnlock()
		return internalerror.StorageClosed
	}
	p, err := store.index.Get(lumpid)
	store.i.RUnlock()
	payload := lumpdata.AsBytes()

	if err != nil {
		// Only one error possible, which is "object could not be found",
		// meaning this is a new object. Padding necessary zeros and call `put`
		toWrite := paddingWithZero(payload, startOffset, reservation)
		data := block.FromBytes(toWrite, block.Min())
		lumpdata = lump.NewLumpDataWithAb(data)
		return store.put(lumpid, lumpdata)
	}

	// update exist object; `reservation` is ignored in this case

	switch v := p.(type) {
	case portion.DataPortion:
		return store.dataRegion.Update(v, startOffset, payload)
	case portion.JournalPortion:
		// TODO?
		return errors.Wrap(internalerror.InvalidInput, "embedt object does not support update")
	default:
		panic("never here")
	}
}

func (store *Storage) PutEmbed(lumpid lump.LumpId, data []byte) (updated bool, err error) {
	if updated, _, err = store.deleteIfExist(lumpid, false); err != nil {
		return
	}

	store.i.Lock()
	defer store.i.Unlock()
	store.jr.Lock()
	defer store.jr.Unlock()
	err = store.journalRegion.RecordEmbed(store.index, lumpid, data)
	return
}

func (store *Storage) Delete(lumpid lump.LumpId) (updated bool, size uint32, err error) {
	updated, size, err = store.deleteIfExist(lumpid, true)
	return
}

func (store *Storage) deleteIfExist(lumpid lump.LumpId, doRecord bool) (bool, uint32, error) {

	store.i.Lock()
	defer store.i.Unlock()
	if !store.opened {
		return false, 0, internalerror.StorageClosed
	}

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
		store.jr.Lock()
		store.journalRegion.RecordDelete(store.index, lumpid)
		store.jr.Unlock()
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
//thread-UNSAFE
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

func (store *Storage) Sync() {
	store.jr.Lock()
	defer store.jr.Unlock()
	if !store.opened {
		return
	}
	store.journalSync()
}

func (store *Storage) Flush() {
	store.jr.Lock()
	defer store.jr.Unlock()
	if !store.opened {
		return
	}
	store.journalRegion.Flush()
}

func (store *Storage) journalSync() {
	store.journalRegion.Sync()
}

func (store *Storage) Close() {
	store.jr.Lock()
	defer store.jr.Unlock()
	store.i.Lock()
	defer store.i.Unlock()
	if !store.opened {
		return
	}
	fmt.Printf("close")
	store.opened = false
	store.updateCapacityStopper.Stop() //will wait goroutine's end
	store.journalSync()
	store.innerNVM.Close()
	store.index.Free()
	store.alloc.Free()
}

func (store *Storage) RunSideJobOnce(countSideJob int) {
	store.i.Lock()
	defer store.i.Unlock()
	store.jr.Lock()
	defer store.jr.Unlock()
	if store.opened == false {
		return
	}
	store.journalRegion.RunSideJobOnce(store.index, countSideJob)
}

//half open range: [start, end)
func (store *Storage) DeleteRange(start lump.LumpId, end lump.LumpId, hasDataPortion bool) error {
	//write a DeleteRange record into journal
	store.i.Lock()
	defer store.i.Unlock()
	if !store.opened {
		return internalerror.StorageClosed
	}
	store.jr.Lock()
	err := store.journalRegion.RecordDeleteRange(store.index, start, end)
	store.jr.Unlock()
	if err != nil {
		return err
	}
	return store.index.RangeIter(start, end, func(id lump.LumpId, p portion.Portion) error {
		p, err := store.index.Get(id)
		if err != nil {
			return err
		}
		store.index.Delete(id)
		switch v := p.(type) {
		case portion.DataPortion:
			store.dataRegion.Release(v)
		}
		return nil

	})
}

//added API for raft log and raft apply
func (store *Storage) GetRecord(lumpid lump.LumpId) (*portion.DataPortion, error) {
	store.i.RLock()
	defer store.i.RUnlock()
	if store.opened == false {
		return nil, internalerror.StorageClosed
	}
	p, err := store.index.Get(lumpid)
	if err != nil {
		return nil, err
	}
	switch v := p.(type) {
	case portion.DataPortion:
		return &v, nil
	default:
		return nil, errors.Errorf("only support DataPortion")
	}
}

/*
func (store *Storage) Has(lumpid lump.LumpId) bool {
	store.i.RLock()
	defer store.i.RUnlock()
	if store.opened == false {
		return false
	}
	_, err := store.index.Get(lumpid)
	if err != nil {
		return false
	}
	return true
}
*/
/*
func (store *Storage) DeleteRecord(lumpid lump.LumpId) error {
	store.i.Lock()
	defer store.i.Unlock()
	if store.opened == false {
		return internalerror.StorageClosed
	}
	store.jr.Lock()
	defer store.jr.Unlock()

	p, err := store.index.Get(lumpid)
	if err != nil {
		return err
	}
	switch p.(type) {
	case portion.DataPortion:
		if ok := store.index.Delete(lumpid); ok == false {
			panic("Delete after Get failed, something bad happend")
		}
		return store.journalRegion.RecordDelete(store.index, lumpid)
	default:
		return errors.Errorf("only support DataPortion")
	}
}
*/

func (store *Storage) WriteRecord(lumpid lump.LumpId, dataPortion portion.DataPortion) error {
	store.i.Lock()
	defer store.i.Unlock()
	store.jr.Lock()
	defer store.jr.Unlock()
	if store.opened == false {
		return internalerror.StorageClosed
	}
	if err := store.journalRegion.RecordPut(store.index, lumpid, dataPortion); err != nil {
		return err
	}
	store.index.InsertDataPortion(lumpid, dataPortion)
	return nil
}
