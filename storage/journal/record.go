package journal

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/portion"
	"github.com/thesues/cannyls-go/util"
	"hash/adler32"
	"io"
)

//TODO: pre caculate checksum for TAG_GO_FRONT, TAG_END_OF_RECORDS
const (
	TAG_END_OF_RECORDS byte = 0
	TAG_GO_TO_FRONT    byte = 1
	TAG_PUT            byte = 3
	TAG_EMBED          byte = 4
	TAG_DELETE         byte = 5
	TAG_DELETE_RANGE   byte = 6
)
const (
	RECORD_HEADER_SIZE   = 1 + 4 // TAG size + Checksum size
	LUMPID_SIZE          = 16
	LENGTH_SIZE          = 2
	PORTION_SIZE         = 5
	END_OF_RECORDS_SIZE  = 1 + 4 //Tag Size + Checksum size //GO_TO_FRONT and END_OF_RECORD
	EMBEDDED_DATA_OFFSET = RECORD_HEADER_SIZE + LUMPID_SIZE + LENGTH_SIZE
)

type JournalRecord interface {
	WriteTo(io.Writer) error
	ExternalSize() uint32
	CheckSum() uint32
	Tag() byte
}

type EndOfRecords struct{}

type GoToFront struct{}

type PutRecord struct {
	LumpID      lump.LumpId
	DataPortion portion.DataPortion
}

type DeleteRecord struct {
	LumpID lump.LumpId
}
type EmbedRecord struct {
	LumpID lump.LumpId
	Data   []byte
}
type DeleteRange struct {
	Start lump.LumpId
	End   lump.LumpId
}

type JournalEntry struct {
	Start  address.Address
	Record JournalRecord
}

func (entry JournalEntry) End() uint64 {
	return entry.Start.AsU64() + uint64(entry.Record.ExternalSize())
}

//

func (record EndOfRecords) WriteTo(writer io.Writer) (err error) {
	return writeRecordHeader(record, writer)
}

func (record EndOfRecords) ExternalSize() uint32 {
	return RECORD_HEADER_SIZE
}

func (record EndOfRecords) CheckSum() uint32 {
	var buf = []byte{TAG_END_OF_RECORDS}
	return adler32.Checksum(buf)
}

func (record EndOfRecords) Tag() byte {
	return TAG_END_OF_RECORDS
}

//
func (record GoToFront) ExternalSize() uint32 {
	return RECORD_HEADER_SIZE
}

func (record GoToFront) WriteTo(writer io.Writer) error {
	return writeRecordHeader(record, writer)
}

func (record GoToFront) CheckSum() uint32 {
	var buf = []byte{TAG_GO_TO_FRONT}
	return adler32.Checksum(buf[:])
}

func (record GoToFront) Tag() byte {
	return TAG_GO_TO_FRONT
}

//
func (record PutRecord) ExternalSize() uint32 {
	return RECORD_HEADER_SIZE + LUMPID_SIZE + LENGTH_SIZE + PORTION_SIZE
}

func (record PutRecord) WriteTo(writer io.Writer) error {
	if err := writeRecordHeader(record, writer); err != nil {
		return err
	}
	if _, err := writer.Write(record.LumpID.GetBytes()); err != nil {
		return err
	}
	offset, len := record.DataPortion.AsInts()
	//len + offset is 7 byte
	var buf [7]byte
	util.PutUINT16(buf[:2], len)
	util.PutUINT40(buf[2:], offset)
	if _, err := writer.Write(buf[:]); err != nil {
		return err
	}
	return nil
}

func (record PutRecord) Tag() byte {
	return TAG_PUT
}

func (record PutRecord) CheckSum() uint32 {
	var tag = []byte{TAG_PUT}
	hash := adler32.New()
	hash.Write(tag)
	hash.Write(record.LumpID.GetBytes())
	offset, len := record.DataPortion.AsInts() //offset is always 40bit wide
	// uint40 + uint16 = 7 bytes
	var buf [7]byte
	util.PutUINT40(buf[:5], offset) //40bit
	util.PutUINT16(buf[5:], len)    //16bit
	hash.Write(buf[:])
	return hash.Sum32()
}

//

func (record DeleteRecord) ExternalSize() uint32 {
	return RECORD_HEADER_SIZE + LUMPID_SIZE
}

func (record DeleteRecord) WriteTo(writer io.Writer) error {
	if err := writeRecordHeader(record, writer); err != nil {
		return err
	}
	if _, err := writer.Write(record.LumpID.GetBytes()); err != nil {
		return err
	}
	return nil
}

func (record DeleteRecord) CheckSum() uint32 {
	var tag = []byte{TAG_DELETE}
	hash := adler32.New()
	hash.Write(tag)
	hash.Write(record.LumpID.GetBytes())
	return hash.Sum32()
}

func (record DeleteRecord) Tag() byte {
	return TAG_DELETE
}

//

func (record EmbedRecord) ExternalSize() uint32 {
	return RECORD_HEADER_SIZE + LUMPID_SIZE + LENGTH_SIZE + uint32(len(record.Data))
}

func (record EmbedRecord) WriteTo(w io.Writer) error {
	if err := writeRecordHeader(record, w); err != nil {
		return err
	}
	if _, err := w.Write(record.LumpID.GetBytes()); err != nil {
		return err
	}

	//len is 2 bytes
	if err := binary.Write(w, binary.BigEndian,
		uint16(len(record.Data))); err != nil {
		return err
	}

	if _, err := w.Write(record.Data); err != nil {
		return err
	}

	return nil

}

func (record EmbedRecord) Tag() byte {
	return TAG_EMBED
}

func (record EmbedRecord) CheckSum() uint32 {
	var tag = []byte{TAG_EMBED}
	hash := adler32.New()
	//tag
	hash.Write(tag)
	//lumpID
	hash.Write(record.LumpID.GetBytes())
	//length of data
	var buf [2]byte
	util.PutUINT16(buf[:], uint16(len(record.Data)))
	hash.Write(buf[:])
	//data
	hash.Write(record.Data)
	return hash.Sum32()
}

//

func (record DeleteRange) CheckSum() uint32 {
	var tag = []byte{TAG_DELETE_RANGE}
	hash := adler32.New()
	//tag
	hash.Write(tag)
	hash.Write(record.Start.GetBytes())
	hash.Write(record.End.GetBytes())
	return hash.Sum32()
}

func (record DeleteRange) WriteTo(w io.Writer) error {
	if err := writeRecordHeader(record, w); err != nil {
		return err
	}
	w.Write(record.Start.GetBytes())
	w.Write(record.End.GetBytes())

	return nil
}

func (record DeleteRange) ExternalSize() uint32 {
	return RECORD_HEADER_SIZE + 2*LUMPID_SIZE
}

func (record DeleteRange) Tag() byte {
	return TAG_DELETE_RANGE
}

//

func ReadFrom(reader io.Reader) (JournalRecord, error) {
	checksum, tag, err := readRecordHeader(reader)
	if err != nil {
		return nil, err
	}
	var record JournalRecord
	var lumpID, start, end lump.LumpId

	switch tag {
	case TAG_END_OF_RECORDS:
		record = EndOfRecords{}
	case TAG_GO_TO_FRONT:
		record = GoToFront{}
	case TAG_PUT:
		if lumpID, err = readLumpId(reader); err != nil {
			return nil, err
		}
		var buf [7]byte
		if _, err := reader.Read(buf[:]); err != nil {
			return nil, err
		}
		dataLen := util.GetUINT16(buf[:2])
		dataOffset := util.GetUINT40(buf[2:])
		portion := portion.NewDataPortion(dataOffset, dataLen)
		record = PutRecord{LumpID: lumpID, DataPortion: portion}
	case TAG_EMBED:
		if lumpID, err = readLumpId(reader); err != nil {
			return nil, err
		}

		var dataLen uint16
		binary.Read(reader, binary.BigEndian, &dataLen)

		data := make([]byte, dataLen)
		if _, err = reader.Read(data); err != nil {
			return nil, err
		}
		record = EmbedRecord{LumpID: lumpID, Data: data}
	case TAG_DELETE:
		if lumpID, err = readLumpId(reader); err != nil {
			return nil, err
		}
		record = DeleteRecord{LumpID: lumpID}
	case TAG_DELETE_RANGE:
		if start, err = readLumpId(reader); err != nil {
			return nil, err
		}
		if end, err = readLumpId(reader); err != nil {
			return nil, err
		}
		record = DeleteRange{Start: start, End: end}

	}

	if checksum != record.CheckSum() {
		return nil, errors.Wrapf(internalerror.StorageCorrupted, "checksum disk: %d , mem: %d", checksum, record.CheckSum())
	}

	return record, nil
}

//helper
func readLumpId(reader io.Reader) (lump.LumpId, error) {
	//128bit
	var buf [16]byte
	if _, err := reader.Read(buf[:]); err != nil {
		return lump.EmptyLump(), err
	}
	return lump.FromBytes(buf[:])
}

func writeRecordHeader(record JournalRecord, writer io.Writer) error {
	//checksum
	checksum := record.CheckSum()
	if err := binary.Write(writer, binary.BigEndian, checksum); err != nil {
		return err
	}
	//tag
	tag := record.Tag()
	return binary.Write(writer, binary.BigEndian, tag)
}

func readRecordHeader(reader io.Reader) (uint32, byte, error) {
	var checksum uint32
	var tag byte
	if err := binary.Read(reader, binary.BigEndian, &checksum); err != nil {
		return 0, 0, errors.Wrap(err, "read checksum failed")
	}
	if err := binary.Read(reader, binary.BigEndian, &tag); err != nil {
		return 0, 0, errors.Wrap(err, "read tag failed")
	}
	return checksum, tag, nil
}
