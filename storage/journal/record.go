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

const (
	TAG_END_OF_RECORDS byte = 0
	TAG_GO_TO_FRONT    byte = 1
	TAG_PUT            byte = 3
	TAG_EMBED          byte = 4
	TAG_DELETE         byte = 5
	TAG_DELETE_RANGE   byte = 6
)

type JournalRecord interface {
	WriteTo(io.Writer) error
	ExternalSize() uint32
	CheckSum() uint32
}

type EndOfRecords struct{}

type GoToFront struct{}

type PutRecord struct {
	LumpID      lump.LumpId
	dataPortion portion.DataPortion
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
	start  address.Address
	record JournalRecord
}

func (record EndOfRecords) WriteTo(writer io.Writer) error {
	var tag = []byte{TAG_END_OF_RECORDS}
	_, err := writer.Write(tag)
	return err
}

func (record EndOfRecords) ExternalSize() uint32 {
	return 0
}

func (record EndOfRecords) CheckSum() uint32 {
	var buf = []byte{TAG_END_OF_RECORDS}
	return adler32.Checksum(buf)
}

//
func (record GoToFront) ExternalSize() uint32 {
	return 0
}

func (record GoToFront) WriteTo(writer io.Writer) error {
	//TODO
	return nil
}

func (record GoToFront) CheckSum() uint32 {
	var buf = []byte{TAG_GO_TO_FRONT}
	return adler32.Checksum(buf[:])
}

//
func (record PutRecord) ExternalSize() uint32 {
	//TODO
	return 0
}

func (record PutRecord) WriteTo(writer io.Writer) error {
	//TODO
	return nil
}

func (record PutRecord) CheckSum() uint32 {
	var tag = []byte{TAG_PUT}
	hash := adler32.New()
	hash.Write(tag)
	hash.Write(record.LumpID.GetBytes())
	offset, len := record.dataPortion.AsInts() //offset is always 40bit wide
	// uint40 + uint16 = 7 bytes
	var buf [7]byte
	util.PutUINT40(buf[:5], offset) //40bit
	util.PutUINT16(buf[5:], len)    //16bit
	hash.Write(buf[:])
	return hash.Sum32()
}

//

func (record DeleteRecord) ExternalSize() uint32 {
	//TODO
	return 0
}

func (record DeleteRecord) WriteTo(writer io.Writer) error {
	//TODO
	return nil
}

func (record DeleteRecord) CheckSum() uint32 {
	var tag = []byte{TAG_DELETE}
	hash := adler32.New()
	hash.Write(tag)
	hash.Write(record.LumpID.GetBytes())
	return hash.Sum32()
}

//

func (record EmbedRecord) ExternalSize() uint32 {
	//TODO
	return 0
}

func (record EmbedRecord) WriteTo(w io.Writer) error {
	return nil

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
	return 0
}

func (record DeleteRange) WriteTo(w io.Writer) error {
	return nil
}

func (record DeleteRange) ExternalSize() uint32 {
	return 0
}

//

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
		record = PutRecord{LumpID: lumpID, dataPortion: portion}
	case TAG_EMBED:
		if lumpID, err = readLumpId(reader); err != nil {
			return nil, err
		}
		var buf [2]byte
		if _, err = reader.Read(buf[:]); err != nil {
			return nil, err
		}
		dataLen := util.GetUINT16(buf[:])
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
		return nil, errors.Wrap(internalerror.StorageCorrupted, "checksum journal record")
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
