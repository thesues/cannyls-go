package journal

import (
	"bufio"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/klauspost/readahead"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/portion"
)

var _ = fmt.Print

type JournalRingBuffer struct {
	//FIXME
	nvm            *JournalNvmBuffer
	unreleasedHead uint64
	head           uint64
	tail           uint64
	//usage field is atomic, only used for collecting metrics
	usage uint64
}

func (ring *JournalRingBuffer) Head() uint64 {
	return ring.head

}

func (ring *JournalRingBuffer) Tail() uint64 {
	return ring.tail
}

func NewJournalRingBuffer(nvm *JournalNvmBuffer, head uint64) *JournalRingBuffer {
	return &JournalRingBuffer{
		nvm:            nvm,
		unreleasedHead: head,
		head:           head,
		tail:           head,
	}
}

func (ring *JournalRingBuffer) isEmpty() bool {
	return ring.head == ring.tail
}

func (ring *JournalRingBuffer) ReadEmbededBuffer(position uint64, data []byte) (err error) {
	if _, err = ring.nvm.Seek(int64(position), io.SeekStart); err != nil {
		return err
	}
	if _, err = ring.nvm.Read(data); err != nil {
		return err
	}
	return
}

func (ring *JournalRingBuffer) Capacity() uint64 {
	return ring.nvm.Capacity()
}

//unsafe
func (ring *JournalRingBuffer) DoStoreUsage() {
	var usage uint64
	if ring.unreleasedHead <= ring.tail {
		usage = ring.tail - ring.unreleasedHead
	} else {
		usage = ring.tail + ring.Capacity() - ring.unreleasedHead
	}
	atomic.StoreUint64(&ring.usage, usage)
}

//go routine safe
func (ring *JournalRingBuffer) Usage() uint64 {
	return atomic.LoadUint64(&ring.usage)
}

func (ring *JournalRingBuffer) Sync() error {
	return ring.nvm.Sync()
}

func (ring *JournalRingBuffer) Flush() error {
	return ring.nvm.Flush()
}

//only return embeded JournalPortion
func (ring *JournalRingBuffer) Enqueue(record JournalRecord) (jportion portion.JournalPortion, err error) {
	jportion = portion.JournalPortion{}
	err = nil
	//1. check usage
	if ring.checkFreeSpace(record) == false {
		err = internalerror.JournalStorageFull
		return
	}

	//2. check overflow
	if ring.isOverFlow(record) {
		if _, err = ring.nvm.Seek(int64(ring.tail), io.SeekStart); err != nil {
			return
		}
		r := GoToFront{}
		if err = r.WriteTo(ring.nvm); err != nil {
			return
		}

		//Jump to front
		ring.tail = 0
		return ring.Enqueue(record)
	}

	//3. write data
	preTail := ring.tail
	if _, err = ring.nvm.Seek(int64(ring.tail), io.SeekStart); err != nil {
		return
	}
	if err = record.WriteTo(ring.nvm); err != nil {
		return
	}
	ring.tail = ring.nvm.Position()

	//4. write End OF Record
	endRecord := EndOfRecords{}
	if err = endRecord.WriteTo(ring.nvm); err != nil {
		return
	}

	switch r := record.(type) {
	case EmbedRecord:
		jportion = portion.NewJournalPortion(preTail+EMBEDDED_DATA_OFFSET, uint16(len(r.Data)))
	}

	ring.DoStoreUsage()
	return
}

func (ring *JournalRingBuffer) checkFreeSpace(record JournalRecord) bool {
	writeEnd := ring.tail + uint64(record.ExternalSize()) + END_OF_RECORDS_SIZE
	writeEnd = ring.nvm.BlockSize().CeilAlign(writeEnd)

	/*
	* freeEnd is the length this record could possiblly reach. It could be overflow
	 */
	var freeEnd uint64
	if ring.tail < ring.unreleasedHead {
		freeEnd = ring.unreleasedHead
	} else {
		freeEnd = ring.Capacity() + ring.unreleasedHead
	}

	//fmt.Printf("Usage: %d, tail = %d, freeEnd = %d\n", ring.Usage(), ring.tail, freeEnd)

	if writeEnd > freeEnd {
		return false
	}
	return true
}

func (ring *JournalRingBuffer) isOverFlow(record JournalRecord) bool {
	writeEnd := ring.tail + uint64(record.ExternalSize()) + END_OF_RECORDS_SIZE
	return writeEnd > ring.nvm.Capacity()
}

func (ring *JournalRingBuffer) ReleaseBytesUntil(head uint64) {
	ring.unreleasedHead = head
	ring.DoStoreUsage()
}

/*No buffer and update head*/
type DequeueIter struct {
	readBuf      *SeekableReader
	ring         *JournalRingBuffer
	isSecondLoop bool
}

func (iter DequeueIter) PopFront() (entry JournalEntry, err error) {
	record, err := ReadRecordFrom(iter.readBuf)
	if err != nil {
		return JournalEntry{}, err
	}
	switch record.(type) {
	case GoToFront:
		if iter.isSecondLoop == true {
			panic("has two GoToFront in journal")
		}
		iter.ring.head = 0
		iter.readBuf.Seek(0, io.SeekStart)
		iter.isSecondLoop = true
		return iter.PopFront()
	case EndOfRecords:
		//this will not update ring.head
		return JournalEntry{}, internalerror.NoEntries
	default:
		entry = JournalEntry{
			Start:  address.AddressFromU64(iter.ring.head),
			Record: record,
		}
		iter.ring.head = entry.End()
		return entry, nil
	}

}

/*Use buffer and update tail*/
type BufferedIter struct {
	fastReader readahead.ReadSeekCloser
	ring       *JournalRingBuffer
}

//Update the ring.tail
func (iter BufferedIter) PopFront() (entry JournalEntry, err error) {
	record, err := ReadRecordFrom(iter.fastReader)
	if err != nil {
		return JournalEntry{}, err
	}
	switch record.(type) {
	case GoToFront:
		iter.ring.tail = 0
		iter.fastReader.Seek(0, io.SeekStart)
		return iter.PopFront()
	case EndOfRecords:
		//this will not update ring.tail
		return JournalEntry{}, internalerror.NoEntries
	default:
		entry = JournalEntry{
			Start:  address.AddressFromU64(iter.ring.tail),
			Record: record,
		}
		iter.ring.tail = entry.End()
		return entry, nil
	}
}

func (iter BufferedIter) Close() {
	iter.fastReader.Close()
}

/*No buffer and update nothing*/
type ReadIter struct {
	ring *JournalRingBuffer
}

/* No buffer and update nothing */
func (iter ReadIter) PopFront() (entry JournalEntry, err error) {
	record, err := ReadRecordFrom(iter.ring.nvm)
	if err != nil {
		return JournalEntry{}, err
	}
	switch record.(type) {
	case GoToFront:
		iter.ring.nvm.Seek(0, 0)
		return iter.PopFront()
	case EndOfRecords:
		//this will not update ring.head
		return JournalEntry{}, internalerror.NoEntries
	default:
		entry = JournalEntry{
			Start:  address.AddressFromU64(iter.ring.head),
			Record: record,
		}
		//current = entry.End()
		return entry, nil
	}
}

/*Use Buffer and update tail*/
func (ring *JournalRingBuffer) BufferedIter() BufferedIter {
	ra, err := readahead.NewReadSeekerSize(ring.nvm, 4, 1<<20)
	if err != nil {
		panic("should not happen in create readahead buf")
	}

	if _, err := ra.Seek(int64(ring.head), 0); err != nil {
		panic(fmt.Sprintf("panic in new DequeueIter %+v", err))
	}
	return BufferedIter{
		ring:       ring,
		fastReader: ra,
	}
}

/*User Buffer and update head*/
func (ring *JournalRingBuffer) DequeueIter() DequeueIter {
	readBuf := createSeekableReader(ring.nvm, 8*1024)
	if _, err := readBuf.Seek(int64(ring.head), 0); err != nil {
		panic(fmt.Sprintf("panic in new DequeueIter %+v", err))
	}

	return DequeueIter{
		ring:         ring,
		readBuf:      readBuf,
		isSecondLoop: false,
	}
}

/*No buffer and update nothing*/
func (ring *JournalRingBuffer) ReadIter() ReadIter {
	if _, err := ring.nvm.Seek(int64(ring.unreleasedHead), 0); err != nil {
		panic(fmt.Sprintf("panic in new DequeueIter %+v", err))
	}
	return ReadIter{
		ring: ring,
	}
}

type SeekableReader struct {
	f nvm.NonVolatileMemory
	*bufio.Reader
}

func createSeekableReader(f nvm.NonVolatileMemory, capacity int) *SeekableReader {
	return &SeekableReader{
		f:      f,
		Reader: bufio.NewReaderSize(f, capacity),
	}
}

func (r *SeekableReader) Seek(offset int64, whence int) (off int64, err error) {
	if whence == 1 {
		offset -= int64(r.Buffered())
	}
	off, err = r.f.Seek(offset, whence)
	if err != nil {
		return 0, err
	}
	r.Reset(r.f)
	return
}
