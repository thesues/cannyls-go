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

func (journal *JournalRegion) SetAutomaticGcMode(gc bool) {
	journal.gcAfterAppend = gc
}

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

	//FIXME
	//if
	ringBuffer := NewJournalNvmBuffer(ringNVM)
	ring := NewJournalRingBuffer(ringBuffer, header)
	//else
	//ring := NewJournalRingBuffer(ringNVM, header)

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

func (journal *JournalRegion) RestoreIndex(index *lumpindex.LumpIndex) {
	var entry JournalEntry
	var err error
	iter := journal.ring.Iter()
	for {
		entry, err = iter.PopItemForRestore()
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
func (journal *JournalRegion) append(index *lumpindex.LumpIndex, record JournalRecord) error {
	var err error
	var embeded portion.JournalPortion
	if embeded, err = journal.ring.Enqueue(record); err != nil {
		return err
	}
	//if record is an embeded entry, we should update the index as well
	//because journal GC start after append, to prevent to be GCed
	switch v := record.(type) {
	case EmbedRecord:
		index.InsertJournalPortion(v.LumpID, embeded)
	}
	return nil
}

func (journal *JournalRegion) appendWithGC(index *lumpindex.LumpIndex, record JournalRecord) (err error) {
	if err = journal.append(index, record); err != nil {
		return err
	}
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
		if journalPortion.Start == entry.Start+EMBEDDED_DATA_OFFSET && int(journalPortion.Len) == len(v.Data) {
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

		} else {
			break
		}
	}

ENDFOR:

	/*
		front := journal.gcQueue.Front()
		if front != nil {
			//assert head == unrelease head
			//head := journal.ring.Head()
			head := front.(JournalEntry).Start
			journal.ring.ReleaseBytesUntil(head.AsU64())
		} else {
			head := journal.ring.Head()
			journal.ring.ReleaseBytesUntil(head)
		}
	*/

}

func (journal *JournalRegion) writeUnusedJournalHeader(head uint64) {
	journal.headerRegion.WriteTo(head)
	journal.ring.ReleaseBytesUntil(head)
}

func (journal *JournalRegion) fillGCQueue() {

	if journal.ring.isEmpty() {
		return
	}
	//assert (ring.unrelease_head == ring.head)
	fmt.Printf("update unused head to %d\n", journal.ring.head)
	journal.writeUnusedJournalHeader(journal.ring.head)
	var i int
	i = 0
	iter := journal.ring.Iter()
	for i < GC_QUEUE_SIZE {
		entry, err := iter.PopFront()
		//fmt.Printf("read entry: %+v, err: %+v\n", entry, err)
		if err == internalerror.NoEntries {
			break
		}
		if err != nil {
			panic(fmt.Sprintf("Journal failed to read entries %+v", err))
		}
		journal.gcQueue.PushBack(entry)
		i++
	}
}

func (journal *JournalRegion) Sync() {
	if err := journal.ring.Sync(); err != nil {
		panic(fmt.Sprintf("journal sync failed: %v", err))
	}
	//journal.headerRegion.WriteTo(journal.ring.unreleasedHead)
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
func (journal *JournalRegion) RecordPut(index *lumpindex.LumpIndex, id lump.LumpId, data portion.DataPortion) error {
	record := PutRecord{
		LumpID:      id,
		DataPortion: data,
	}
	return journal.appendWithGC(index, record)
}

//WARNING: this will update the INDEX
func (journal *JournalRegion) RecordEmbed(index *lumpindex.LumpIndex, id lump.LumpId, data []byte) error {
	record := EmbedRecord{
		LumpID: id,
		Data:   data,
	}
	return journal.appendWithGC(index, record)
}

func (journal *JournalRegion) RecordDelete(index *lumpindex.LumpIndex, id lump.LumpId) error {
	record := DeleteRecord{
		LumpID: id,
	}
	return journal.appendWithGC(index, record)
}

func (journal *JournalRegion) RecordDeleteRange(index *lumpindex.LumpIndex, start, end lump.LumpId) error {
	record := DeleteRange{
		Start: start,
		End:   end,
	}
	return journal.appendWithGC(index, record)
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

func (journal *JournalRegion) GetEmbededData(embeded portion.JournalPortion) (buf []byte, err error) {
	buf = make([]byte, embeded.Len)
	if err = journal.ring.ReadEmbededBuffer(embeded.Start.AsU64(), buf); err != nil {
		return nil, err
	}
	return
}

func (journal *JournalRegion) gcAllEntriesInQueue(index *lumpindex.LumpIndex) {
	for journal.gcQueue.Len() != 0 {
		journal.gcOnce(index)
	}
}

func (journal *JournalRegion) JournalEntries() (uint64, uint64, uint64, []JournalEntry) {
	entries := make([]JournalEntry, 0)
	iter := journal.ring.Iter()
	for {
		entry, err := iter.PopFrontWithoutUpdate()
		if err == internalerror.NoEntries {
			break
		}
		if err != nil {
			panic(fmt.Sprintf("Journal failed to read entries"))
		}
		entries = append(entries, entry)
	}
	return journal.ring.unreleasedHead, journal.ring.head, journal.ring.tail, entries
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
	journal.writeUnusedJournalHeader(journal.ring.Head())
	//assert head == unreleased_head
	//journal.headerRegion.WriteTo(journal.ring.Head())
	//journal.Sync()
}

//I do not understand this!
func between(x, y, z uint64) bool {
	return (x <= y && y <= z) || (z <= x && x <= y) || (y <= z && z <= x)
}
