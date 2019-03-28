package journal

import (
	"encoding/binary"
	"fmt"
	"github.com/phf/go-queue/queue"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/lumpindex"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/portion"
	"io"
)

const (
	GC_COUNT_IN_SIDE_JOB = 64
	GC_QUEUE_SIZE        = 0x1000
	SYNC_INTERVAL        = 0x1000
)

type JournalRegion struct {
	headerRegion  *JournalHeaderRegion
	ring          *JournalRingBuffer
	gcQueue       *queue.Queue
	syncCountDown int
	gcAfterAppend bool
}

/*
func InitialJournalRegion(nvm nvm.NonVolatileMemory) {
	header := NewJournalHeadRegion(nvm)
	if err := header.WriteTo(0); err != nil {
		panic("failed to initialize JournalRegion")
	}
	r := EndOfRecords{}
	ab := block.NewAlignedBytes(512, nvm.BlockSize())
	if err := r.WriteTo(nvm); err != nil {
		panic("failed to initialize JournalRegion")
	}
}
*/

//This writer is not direct-io
func InitialJournalRegion(writer io.Writer, sector block.BlockSize) {
	//journal header
	padding := sector.AsU16() - 8
	var buf = make([]byte, padding)
	binary.Write(writer, binary.BigEndian, uint64(0))
	writer.Write(buf)

	r := EndOfRecords{}
	if err := r.WriteTo(writer); err != nil {
		panic("failed to initialize JournalRegion")
	}
}

func OpenJournalRegion(nvm nvm.NonVolatileMemory) (*JournalRegion, error) {

	blockSize := nvm.BlockSize()

	headerNVM, ringNVM, err := nvm.Split(uint64(blockSize.AsU16()))
	if err != nil {
		return nil, err
	}

	headerRegion := NewJournalHeadRegion(headerNVM)
	header, err := headerRegion.ReadFrom()
	if err != nil {
		return nil, err
	}

	ringBuffer := NewJournalNvmBuffer(ringNVM)
	ring := NewJournalRingBuffer(ringBuffer, header)

	q := queue.New()
	q.Init()
	return &JournalRegion{
		headerRegion:  headerRegion,
		ring:          ring,
		gcQueue:       q,
		syncCountDown: SYNC_INTERVAL,
		gcAfterAppend: true,
	}, nil
}

func (journal *JournalRegion) RestoreIndex(index lumpindex.LumpIndex) {
	var entry JournalEntry
	var err error
	for {
		entry, err = journal.ring.PopItemForRestore()
		if err != nil {
			if err != internalerror.NoEntries {
				panic(fmt.Sprintf("Can not restore journal :%v", err))
			}
			break
		}
		switch record := entry.Record.(type) {
		case PutRecord:
			index.InsertDataPortion(record.LumpID, record.DataPortion)
		case EmbedRecord:
			portionOnJournal := portion.NewJournalPortion(entry.Start.AsU64()+EMBEDDED_DATA_OFFSET, uint16(len(record.Data)))
			index.InsertJournalPortion(record.LumpID, portionOnJournal)
		case DeleteRange:
			index.DeleteRange(record.Start, record.End)
		case DeleteRecord:
			index.Delete(record.LumpID)
		case EndOfRecords, GoToFront:
			panic("read out an unexpected record")
		default:
			panic("never be here")
		}
	}
}
func (journal *JournalRegion) append(index *lumpindex.LumpIndex, record JournalRecord) {
	var err error
	var embeded portion.JournalPortion
	if embeded, err = journal.ring.Enqueue(record); err != nil {
		panic(fmt.Sprintf("Write record failed %v", err))
	}
	//if record is an embeded entry, we should update the index as well
	switch v := record.(type) {
	case EmbedRecord:
		index.InsertJournalPortion(v.LumpID, embeded)
	}
}

func (journal *JournalRegion) appendWithGC(index *lumpindex.LumpIndex, record JournalRecord) (jbportion portion.JournalPortion) {

	journal.append(index, record)
	if journal.gcAfterAppend {
		journal.gcOnce(index)
	}
	journal.trySync()
	return
}

func (Journal *JournalRegion) isGarbage(index *lumpindex.LumpIndex, entry JournalEntry) bool {
	var dataPortion portion.DataPortion
	var journalPortion portion.JournalPortion
	var p portion.Portion
	var err error
	record := entry.Record.(JournalRecord)
	switch v := record.(type) {
	case PutRecord:
		if p, err = index.Get(v.LumpID); err != nil {
			return true
		}
		dataPortion = p.(portion.DataPortion)
		return dataPortion != v.DataPortion
	case EmbedRecord:
		if p, err = index.Get(v.LumpID); err != nil {
			return true
		}
		journalPortion = p.(portion.JournalPortion)
		if journalPortion.Start == entry.Start && int(journalPortion.Len) == len(v.Data) {
			return false
		} else {
			return true
		}
	default:
		return true
	}
}

func (journal *JournalRegion) gcOnce(index *lumpindex.LumpIndex) {
	if journal.gcQueue.Len() == 0 && journal.ring.Capacity() < journal.ring.Usage()*2 {
		journal.fillGCQueue()
	}

	for {
		if e := journal.gcQueue.PopFront(); e != nil {
			entry := e.(JournalEntry)

			if journal.isGarbage(index, entry) == false {
				record := entry.Record
				journal.append(index, record)
				goto ENDFOR
			}

		}
	}

ENDFOR:

	if journal.gcQueue.Len() == 0 {
		//head == unrelease head
		head := journal.ring.Head()
		journal.ring.ReleaseBytesUntil(head)
	} else {
		//we have processoed at lease one journal, update unreleased head
		e := journal.gcQueue.Front()
		r := e.(JournalEntry)
		head := r.Start.AsU64()
		journal.ring.ReleaseBytesUntil(head)
	}

}

func (journal *JournalRegion) fillGCQueue() {

	if journal.ring.isEmpty() {
		return
	}

	//assert (ring.unrelease_head == ring.head)

	var i int
	i = 0
	for i < GC_QUEUE_SIZE {
		entry, err := journal.ring.PopFront()
		if err == internalerror.NoEntries {
			break
		}
		if err != nil {
			panic(fmt.Sprintf("Journal failed to read entries"))
		}
		journal.gcQueue.PushBack(entry)
		i++
	}

	if journal.gcQueue.Len() != 0 {
		journal.headerRegion.WriteTo(journal.ring.unreleasedHead)
	}

	/*
		if i > 0 {
			firstEntry := journal.gcQueue.Front().(JournalEntry)
			if err := journal.headerRegion.WriteTo(firstEntry.Start.AsU64()); err != nil {
				panic("sync journal header failed")
			}
		}
	*/
}

func (journal *JournalRegion) Sync() {
	if err := journal.ring.Sync(); err != nil {
		panic(fmt.Sprintf("journal sync failed: %v", err))
	}
	journal.syncCountDown = SYNC_INTERVAL
}

func (journal *JournalRegion) trySync() {
	if journal.syncCountDown <= 0 {
		journal.Sync()
	} else {
		journal.syncCountDown -= 1
	}
}

//Write Journal, Update Index
func (journal *JournalRegion) RecordPut(index *lumpindex.LumpIndex, id lump.LumpId, data portion.DataPortion) {
	record := PutRecord{
		LumpID:      id,
		DataPortion: data,
	}
	journal.appendWithGC(index, record)
}

//WARNING: this will update the INDEX
func (journal *JournalRegion) RecordEmbed(index *lumpindex.LumpIndex, id lump.LumpId, data []byte) {
	record := EmbedRecord{
		LumpID: id,
		Data:   data,
	}
	journal.appendWithGC(index, record)
}

func (journal *JournalRegion) RecordDelete(index *lumpindex.LumpIndex, id lump.LumpId) {
	record := DeleteRecord{
		LumpID: id,
	}
	journal.appendWithGC(index, record)
}

func (journal *JournalRegion) RecordDeleteRange(index *lumpindex.LumpIndex, start, end lump.LumpId) {
	record := DeleteRange{
		Start: start,
		End:   end,
	}
	journal.appendWithGC(index, record)

}

func (journal *JournalRegion) RunSideJobOnce(index *lumpindex.LumpIndex) {
	if journal.gcQueue.Len() == 0 {
		journal.fillGCQueue()
	} else if journal.syncCountDown != SYNC_INTERVAL {
		journal.Sync()
	} else {
		for i := 0; i < GC_COUNT_IN_SIDE_JOB; i++ {
			journal.gcOnce(index)
		}
	}
}

func (journal *JournalRegion) GetEmbededData(embeded portion.JournalPortion) []byte {
	buf := make([]byte, embeded.Len)
	journal.ring.ReadEmbededBuffer(embeded.Start.AsU64(), buf)
	return buf
}

func (journal *JournalRegion) gcAllEntriesInQueue(index *lumpindex.LumpIndex) {
	for journal.gcQueue.Len() != 0 {
		journal.gcOnce(index)
	}
}

//maybe sync
func (journal *JournalRegion) GcAllEntries(index *lumpindex.LumpIndex) {
	tail := journal.ring.Tail()
	for {
		before_head := journal.ring.Head()

		if journal.gcQueue.Len() == 0 {
			journal.fillGCQueue()
		}

		journal.gcAllEntriesInQueue(index)

		if between(before_head, tail, journal.ring.Head()) {
			break
		}
	}
	//assert head == unreleased_head
	journal.headerRegion.WriteTo(journal.ring.Head())
}

//I do not understand this!
func between(x, y, z uint64) bool {
	return (x <= y && y <= z) || (z <= x && x <= y) || (y <= z && z <= x)
}
