package nvm

import (
	"encoding/binary"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"io"
	"os"
)

/*
       0                   1                   2                   3
       0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                         Magic Number                          |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |        Header Size            |      Major Version            |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |        Minor Version          |      Block Size               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                                                               |
      |                     Instance UUID (128 bit)                   |
      |                                                               |
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                     Journal Region Size (64 bit)              |
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                     Data Region Size (64 bit)                 |
      |                                                               |
      +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
      |                     Padding (Variable)
	  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
*/

const (
	HEADER_SIZE uint16 = 2 /* major_version */ +
		2 /* minor_version */ +
		2 /* block_size */ +
		16 /* UUID */ +
		8 /* journal_region_size */ +
		8 /* data_region_size */
	FULL_HEADER_SIZE uint16 = 4 + 2 + HEADER_SIZE
)

type StorageHeader struct {
	MajorVersion      uint16
	MinorVersion      uint16
	BlockSize         block.BlockSize
	UUID              uuid.UUID
	JournalRegionSize uint64
	DataRegionSize    uint64
}

func DefaultStorageHeader() *StorageHeader {
	uuid, _ := uuid.NewV4()
	return &StorageHeader{
		MajorVersion:      MAJOR_VERSION,
		MinorVersion:      MINOR_VERSION,
		BlockSize:         block.Min(),
		UUID:              uuid,
		JournalRegionSize: 1024,
		DataRegionSize:    4096,
	}
}
func ReadFromFile(f *os.File) (*StorageHeader, error) {
	return ReadFrom(f)
}

func ReadFrom(reader io.Reader) (*StorageHeader, error) {

	//magic number
	var magicNumber [4]byte
	if n, err := reader.Read(magicNumber[:]); err != nil {
		return nil, err
	} else if n != len(magicNumber) {
		return nil, errors.Wrap(internalerror.InvalidInput, "read magic number")

	} else if magicNumber != MAGIC_NUMBER {
		return nil, errors.Wrap(internalerror.InvalidInput, "read magic number")
	}

	//header size
	var headerSize uint16
	if err := binary.Read(reader, binary.BigEndian, &headerSize); err != nil {
		return nil, internalerror.InvalidInput
	}

	reader = io.LimitReader(reader, int64(headerSize))

	//major version

	var majorVersion uint16
	if err := binary.Read(reader, binary.BigEndian, &majorVersion); err != nil {
		return nil, internalerror.InvalidInput
	} else if majorVersion != MAJOR_VERSION {
		return nil, internalerror.InvalidInput
	}

	// minor version
	var minorVersion uint16
	if err := binary.Read(reader, binary.BigEndian, &minorVersion); err != nil {
		return nil, internalerror.InvalidInput
	} else if minorVersion != MAJOR_VERSION {
		return nil, internalerror.InvalidInput
	}

	// block size
	var bs uint16
	var blockSize block.BlockSize

	if err := binary.Read(reader, binary.BigEndian, &bs); err != nil {
		return nil, internalerror.InvalidInput
	} else if blockSize, err = block.NewBlockSize(bs); err != nil {
		return nil, internalerror.InvalidInput
	}

	// UUID
	var uuidBuf [16]byte
	var fileUUID uuid.UUID
	if n, err := reader.Read(uuidBuf[:]); err != nil {
		return nil, err
	} else if n != len(uuidBuf) {
		return nil, internalerror.InvalidInput
	}

	fileUUID, err := uuid.FromBytes(uuidBuf[:])
	if err != nil {
		return nil, internalerror.InvalidInput
	}

	//journal region size
	//data region size
	var journalRegionSize uint64
	var dataRegionSize uint64
	if err := binary.Read(reader, binary.BigEndian, &journalRegionSize); err != nil {
		return nil, internalerror.InvalidInput
	}
	if err := binary.Read(reader, binary.BigEndian, &dataRegionSize); err != nil {
		return nil, internalerror.InvalidInput
	}

	if journalRegionSize > MAX_JOURNAL_REGION_SIZE || dataRegionSize > MAX_DATA_REGION_SIZE {
		return nil, internalerror.InvalidInput
	}

	//EOF
	var buf [1]byte
	if _, err = reader.Read(buf[:]); err != io.EOF {
		return nil, internalerror.StorageCorrupted
	}

	sh := &StorageHeader{
		MajorVersion:      majorVersion,
		MinorVersion:      minorVersion,
		BlockSize:         blockSize,
		UUID:              fileUUID,
		JournalRegionSize: journalRegionSize,
		DataRegionSize:    dataRegionSize,
	}
	return sh, nil

}

func (self *StorageHeader) WriteTo(writer io.Writer) (err error) {
	err = nil

	//MAGIC NUMBER
	if _, err = writer.Write(MAGIC_NUMBER[:]); err != nil {
		return err
	}
	//Header Size
	if err = binary.Write(writer, binary.BigEndian, HEADER_SIZE); err != nil {
		return err
	}

	//Major Version
	if err = binary.Write(writer, binary.BigEndian, self.MajorVersion); err != nil {
		return err
	}

	//Minor Version
	if err = binary.Write(writer, binary.BigEndian, self.MinorVersion); err != nil {
		return err
	}

	//Block Size
	if err = binary.Write(writer, binary.BigEndian, self.BlockSize.AsU16()); err != nil {
		return err
	}

	//UUID
	if _, err = writer.Write(self.UUID.Bytes()); err != nil {
		return err
	}

	//Journal Region Size

	if err = binary.Write(writer, binary.BigEndian, self.JournalRegionSize); err != nil {
		return err
	}

	//Data Region Size
	if err = binary.Write(writer, binary.BigEndian, self.DataRegionSize); err != nil {
		return err
	}

	return
}

func (self *StorageHeader) RegionSize() uint64 {
	return self.BlockSize.CeilAlign(uint64(FULL_HEADER_SIZE))
}

func (self *StorageHeader) StorageSize() uint64 {
	return self.RegionSize() + self.JournalRegionSize + self.DataRegionSize
}

func (self *StorageHeader) WriteHeaderRegionTo(writer io.Writer) (err error) {
	if err = self.WriteTo(writer); err != nil {
		return
	}

	padding := make([]byte, self.RegionSize()-uint64(FULL_HEADER_SIZE))
	if _, err = writer.Write(padding); err != nil {
		return
	}
	return
}

func (self *StorageHeader) SplitRegion(nvm NonVolatileMemory) (NonVolatileMemory, NonVolatileMemory) {
	headEnd := self.RegionSize()
	_, body := nvm.Split(headEnd)
	journalNVM, dataNVM := body.Split(self.JournalRegionSize)
	return journalNVM, dataNVM
}
