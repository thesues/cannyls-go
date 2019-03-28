package journal

import (
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/portion"
	"io"
)

type JournalRingBuffer struct {
	nvm            *JournalNvmBuffer
	unreleasedHead uint64
	head           uint64
	tail           uint64
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

func (ring *JournalRingBuffer) Usage() uint64 {
	if ring.unreleasedHead <= ring.tail {
		return ring.tail - ring.unreleasedHead
	} else {
		return ring.tail + ring.Capacity() - ring.unreleasedHead
	}
}

func (ring *JournalRingBuffer) Sync() error {
	return ring.nvm.Sync()
}

//only return embeded JournalPortion
func (ring *JournalRingBuffer) Enqueue(record JournalRecord) (jportion portion.JournalPortion, err error) {
	jportion = portion.JournalPortion{}
	err = nil
	//1. check usage
	if ring.checkFreeSpace(record) == false {
		err = internalerror.StorageFull
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

	if writeEnd > freeEnd {
		return false
	}
	return true
}

func (ring *JournalRingBuffer) isOverFlow(record JournalRecord) bool {
	writeEnd := ring.tail + uint64(record.ExternalSize()) + END_OF_RECORDS_SIZE
	return writeEnd > ring.nvm.Capacity()
}

//Update the ring.head
func (ring *JournalRingBuffer) PopFront() (entry JournalEntry, err error) {
	ring.nvm.Seek(int64(ring.head), io.SeekStart)
	record, err := ReadFrom(ring.nvm)
	switch record.(type) {
	case GoToFront:
		ring.head = 0
		ring.nvm.Seek(0, io.SeekStart)
		return ring.PopFront()
	case EndOfRecords:
		//this will not update ring.head
		return JournalEntry{}, internalerror.NoEntries
	default:
		entry = JournalEntry{
			Start:  address.AddressFromU64(ring.head),
			Record: record,
		}
		ring.head = entry.End()
		return entry, nil
	}
}

//Update the ring.tail
func (ring *JournalRingBuffer) PopItemForRestore() (entry JournalEntry, err error) {
	ring.nvm.Seek(int64(ring.tail), io.SeekStart)
	record, err := ReadFrom(ring.nvm)
	switch record.(type) {
	case GoToFront:
		ring.tail = 0
		ring.nvm.Seek(0, io.SeekStart)
		return ring.PopFront()
	case EndOfRecords:
		//this will not update ring.tail
		return JournalEntry{}, internalerror.NoEntries
	default:
		entry = JournalEntry{
			Start:  address.AddressFromU64(ring.tail),
			Record: record,
		}
		ring.tail = entry.End()
		return entry, nil
	}
}

func (ring *JournalRingBuffer) ReleaseBytesUntil(head uint64) {
	ring.unreleasedHead = head
}

/*
func (ring *JournalRingBuffer) DequeueIter() *ReadEntries {
	return NewReadEntries(ring, ring.head)
}

//https://blog.kowalczyk.info/article/1Bkr/3-ways-to-iterate-in-go.html
type ReadEntries struct {
	ring        *JournalRingBuffer
	current     uint64
	isSecondLap bool
}

func newReadEntries(ring *JournalRingBuffer, head uint64) *ReadEntries {
	return &ReadEntries{
		//buf:         bufio.NewReader(nvm),
		ring:        ring,
		current:     head,
		isSecondLap: false,
	}
}

//Skip GoToFront and  return nil if met EndOfRecords
//TODO needs a bufreader
func (reader *ReadEntries) readOneRecord() (record JournalRecord, err error) {
	reader.ring.nvm.Seek(int64(reader.current), io.SeekStart)
	record, err = ReadFrom(reader.ring.nvm)
	switch record.(type) {
	case GoToFront:
		if reader.isSecondLap {
			panic("is second lap")
		}
		reader.ring.nvm.Seek(0, io.SeekStart)
		//reader.buf = bufio.NewReader(reader.nvm)
		reader.current = 0
		reader.isSecondLap = true
		return reader.readOneRecord()
	case EndOfRecords:
		return nil, nil
	default:
		return record, err
	}
}

//If err == internalerror.NoEntries, this is the end of the entry
func (reader *ReadEntries) Next() (record JournalEntry, err error) {
	record = JournalEntry{}
	err = nil
	r, err := reader.readOneRecord()
	if err != nil {
		return
	} else if r == nil {
		//end of records
		err = internalerror.NoEntries
		return
	} else {
		//normal
		record = JournalEntry{
			Start:  address.AddressFromU64(reader.current),
			Record: r,
		}
		reader.current += uint64(r.ExternalSize())
		//reader.ring.head = reader.current
	}
	return
}
*/
