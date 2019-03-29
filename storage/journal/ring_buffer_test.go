package journal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/portion"
	"testing"
)

var _ = fmt.Print

func TestRingBufferAppend(t *testing.T) {
	f, _ := nvm.New(1024 * 1024)
	bufferedNvm := NewJournalNvmBuffer(f)

	ring := NewJournalRingBuffer(bufferedNvm, 0)

	cases := []JournalRecord{
		recordPut("0066", 2134, 5),
		recordPut("11FF", 100, 300),
		recordDelete("2222"),
		recordEmbed("3333", []byte("foo")),
		recordDelete("4444"),
		recordDeleteRange("0000", "9999"),
	}

	for _, c := range cases {
		_, err := ring.Enqueue(c)
		assert.Nil(t, err)
	}

	//fmt.Printf("storage :%v\n", f.AsBytes()[:200])

	i := 0
	var entry JournalEntry
	var err error
	var position uint64 = 0
	//ring.ResetReadBufToHead()
	iter := ring.Iter()
	for {
		if entry, err = iter.PopFront(); err != nil {
			assert.Equal(t, err, internalerror.NoEntries)
			break
		}
		assert.Equal(t, cases[i], entry.Record)
		i++
		assert.Equal(t, position, entry.Start.AsU64())
		position += uint64(entry.Record.ExternalSize())
	}

	assert.Equal(t, uint64(0), ring.unreleasedHead)
	assert.Equal(t, ring.Head(), position)
	assert.Equal(t, ring.Tail(), position)
}
func TestRingBufferEmbeded(t *testing.T) {
	f, _ := nvm.New(1024)
	bufferedNvm := NewJournalNvmBuffer(f)
	ring := NewJournalRingBuffer(bufferedNvm, 0)
	_, err := ring.Enqueue(recordPut("0000", 30, 50))
	assert.Nil(t, err)
	_, err = ring.Enqueue(recordDelete("1111"))
	assert.Nil(t, err)

	embedPortion, err := ring.Enqueue(recordEmbed("2222", []byte("foo")))
	assert.Nil(t, err)

	buf := make([]byte, embedPortion.SizeOnDisk(block.Min()))
	ring.ReadEmbededBuffer(embedPortion.Start.AsU64(), buf)
	assert.Equal(t, []byte("foo"), buf)
}
func TestRingBufferRound(t *testing.T) {
	f, _ := nvm.New(1024)
	bufferedNvm := NewJournalNvmBuffer(f)
	ring := NewJournalRingBuffer(bufferedNvm, 512)
	assert.Equal(t, uint64(512), ring.Head())
	assert.Equal(t, uint64(512), ring.Tail())

	record := recordDelete("0000")
	n := 512 / (record.ExternalSize())
	var i uint32 = 1
	for i <= n {
		ring.Enqueue(record)
		assert.Equal(t, uint64(512+i*21), ring.Tail())
		i++
	}

	assert.Equal(t, uint64(1016), ring.Tail())

	ring.Enqueue(record)
	assert.Equal(t, uint64(21), ring.Tail())

}

func TestRingBufferFull(t *testing.T) {
	f, _ := nvm.New(1024)
	bufferedNvm := NewJournalNvmBuffer(f)
	ring := NewJournalRingBuffer(bufferedNvm, 0)

	record := recordPut("1111", 1, 2)
	for uint32(ring.Tail()) <= 1024-record.ExternalSize() {
		ring.Enqueue(record)
	}
	assert.Equal(t, uint64(1008), ring.Tail())

	_, err := ring.Enqueue(record)
	assert.Error(t, err)

	ring.unreleasedHead = 511
	ring.head = 511
	_, err = ring.Enqueue(record)
	assert.Error(t, err)

	ring.unreleasedHead = 512
	ring.head = 512
	_, err = ring.Enqueue(record)
	assert.Nil(t, err)
	assert.Equal(t, uint32(ring.tail), record.ExternalSize())

}

func TestRingBufferTooLargeRecord(t *testing.T) {
	f, _ := nvm.New(1024)
	bufferedNvm := NewJournalNvmBuffer(f)
	ring := NewJournalRingBuffer(bufferedNvm, 0)

	data := make([]byte, 997)
	record := recordEmbed("1111", data)
	assert.Equal(t, 1020, int(record.ExternalSize()))

	_, err := ring.Enqueue(record)
	assert.Error(t, err)

	data = make([]byte, 996)
	record = recordEmbed("1111", data)
	assert.Equal(t, 1019, int(record.ExternalSize()))
	_, err = ring.Enqueue(record)
	assert.Nil(t, err)

}

func recordPut(lumpID string, start uint64, len uint16) JournalRecord {
	l, _ := lump.FromString(lumpID)
	return PutRecord{
		LumpID:      l,
		DataPortion: portion.NewDataPortion(start, len),
	}
}

func recordEmbed(lumpID string, data []byte) JournalRecord {
	l, _ := lump.FromString(lumpID)
	return EmbedRecord{
		LumpID: l,
		Data:   data,
	}
}

func recordDelete(id string) JournalRecord {
	l, _ := lump.FromString(id)
	return DeleteRecord{
		LumpID: l,
	}
}

func recordDeleteRange(start string, end string) JournalRecord {
	l0, _ := lump.FromString(start)
	l1, _ := lump.FromString(end)
	return DeleteRange{
		Start: l0,
		End:   l1,
	}
}
