package journal

import (
	"fmt"
	"github.com/phf/go-queue/queue"
	_ "github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/lumpindex"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/portion"
	_ "io"
)

const (
	GC_COUNT_IN_SIDE_JOB = 64
)

type JournalRegion struct {
	headerRegion  *JournalHeaderRegion
	ring          *JournalRingBuffer
	gcQueue       *queue.Queue
	syncCountDown int
	gcAfterAppend bool
}

func InitialJournalRegion(nvm nvm.NonVolatileMemory) {
	header := NewJournalHeadRegion(nvm)
	if err := header.WriteTo(0); err != nil {
		panic("failed to initialize JournalRegion")
	}
	r := EndOfRecords{}
	if err := r.WriteTo(nvm); err != nil {
		panic("failed to initialize JournalRegion")
	}
}

func OpenJournalRegion(nvm nvm.NonVolatileMemory, index *lumpindex.LumpIndex) (*JournalRegion, error) {

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
		syncCountDown: 3,
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

func (journal *JournalRegion) appendWithGC(index lumpindex.LumpIndex, record JournalRecord) (jbportion portion.JournalPortion) {
	var err error
	if jbportion, err = journal.ring.Enqueue(record); err != nil {
		panic(fmt.Sprintf("Write record failed %v", err))
	}
	if journal.gcAfterAppend {
		journal.gcOnce(index)
	}
	journal.trySync()
	return
}

func (journal *JournalRegion) gcOnce(index lumpindex.LumpIndex) {

	if journal.gcQueue.Len() == 0 && journal.ring.Capacity() < journal.ring.Usage()*2 {
		journal.fillGCQueue()
	}

}

func (journal *JournalRegion) fillGCQueue() {

	if journal.ring.isEmpty() {
		return
	}

	var i int
	i = 0
	//FIXME
	for i < 30 {
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

	//read out some data
	if i > 0 {
		firstEntry := journal.gcQueue.Front().(JournalEntry)
		if err := journal.headerRegion.WriteTo(firstEntry.Start.AsU64()); err != nil {
			panic("sync journal header failed")
		}
	}

}

func (journal *JournalRegion) trySync() {
	if journal.syncCountDown == 0 {
		journal.syncCountDown = 3 //FIXME
		if err := journal.ring.Sync(); err != nil {
			panic(fmt.Sprintf("journal sync failed: %v", err))
		}
	} else {
		journal.syncCountDown -= 1
	}
}

//Write Journal, Update Index
func (journal *JournalRegion) RecordPut(index lumpindex.LumpIndex, id lump.LumpId, data portion.DataPortion) {
	record := PutRecord{
		LumpID:      id,
		DataPortion: data,
	}
	journal.appendWithGC(index, record)
	index.InsertDataPortion(id, data)
}

func (journal *JournalRegion) RecordEmbed(index lumpindex.LumpIndex, id lump.LumpId, data []byte) {
	record := EmbedRecord{
		LumpID: id,
		Data:   data,
	}
	embededPortion := journal.appendWithGC(index, record)
	index.InsertJournalPortion(id, embededPortion)
}

func (journal *JournalRegion) RecordDelete(index lumpindex.LumpIndex, id lump.LumpId) {
	record := DeleteRecord{
		LumpID: id,
	}
	journal.appendWithGC(index, record)
	index.Delete(id)
}

func (journal *JournalRegion) RecordDeleteRange(index lumpindex.LumpIndex, start, end lump.LumpId) {
	record := DeleteRange{
		Start: start,
		End:   end,
	}
	journal.appendWithGC(index, record)

	//this call is buggy
	index.DeleteRange(start, end)
}
