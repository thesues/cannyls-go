package snapnvm

import (
	_ "bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/util"
	judy "github.com/thesues/go-judy"

	"hash/adler32"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/nvm"
)

var (
	MAGIC_NUMBER         = [4]byte{'s', 'n', 'a', 'p'}
	MAJOR_VERSION uint16 = 0
	MINOR_VERSION uint16 = 1
)

//Default RegionShift is 25 : 1<<25 == 32MB
type BackingFileHeader struct {
	MajorVersion uint16
	MinorVersion uint16
	UUID         uuid.UUID
	RegionShift  uint16
	MaxCapacity  uint64
	JournalSize  uint64
}

//checkSum + tag + 4+4
//checkSum + tag
const (
	TAG_END_BITMAP byte = 0
	TAG_BITMAP     byte = 1
)

type Bitmap interface {
	WriteTo(io.Writer) error
	CheckSum() uint32
	Tag() byte
}

type EndOfBitmap struct{}

func (EndOfBitmap) Tag() byte {
	return TAG_END_BITMAP
}
func (end EndOfBitmap) CheckSum() uint32 {
	return 65537
}
func (end EndOfBitmap) WriteTo(writer io.Writer) (err error) {
	err = binary.Write(writer, binary.BigEndian, end.CheckSum())
	err = binary.Write(writer, binary.BigEndian, TAG_END_BITMAP)
	return
}

//the unit is fraction of "1 << RegionShift" , default is 32MB
type BitmapEntry struct {
	OriginOffset uint32
	SnapOffset   uint32
}

func (entry BitmapEntry) Tag() byte {
	return TAG_BITMAP
}

func (entry BitmapEntry) CheckSum() uint32 {
	var tag = []byte{TAG_BITMAP}
	hash := adler32.New()
	hash.Write(tag)
	var buf [8]byte
	binary.BigEndian.PutUint32(buf[:4], entry.OriginOffset)
	binary.BigEndian.PutUint32(buf[4:], entry.SnapOffset)
	hash.Write(buf[:])
	return hash.Sum32()
}

func (entry BitmapEntry) WriteTo(writer io.Writer) (err error) {
	if err = binary.Write(writer, binary.BigEndian, entry.CheckSum()); err != nil {
		return
	}
	if err = binary.Write(writer, binary.BigEndian, TAG_BITMAP); err != nil {
		return
	}
	if err = binary.Write(writer, binary.BigEndian, entry.OriginOffset); err != nil {
		return
	}
	if err = binary.Write(writer, binary.BigEndian, entry.SnapOffset); err != nil {
		return
	}
	return
}

func ReadRecordFrom(reader io.Reader) Bitmap {
	var buf [5]byte
	if _, err := io.ReadFull(reader, buf[:]); err != nil {
		panic("can not read from disk: from ReadRecordFrom")
	}
	checksum := binary.BigEndian.Uint32(buf[:4])
	tag := buf[4]
	var bitmap Bitmap
	switch tag {
	case TAG_END_BITMAP:
		bitmap = EndOfBitmap{}
	case TAG_BITMAP:
		var originOffset uint32
		var snapOffset uint32
		binary.Read(reader, binary.BigEndian, &originOffset)
		binary.Read(reader, binary.BigEndian, &snapOffset)
		bitmap = BitmapEntry{OriginOffset: originOffset, SnapOffset: snapOffset}
	default:
		panic("not the tag we want")
	}
	//check entry's crc
	if bitmap.CheckSum() != checksum {
		panic("check sum failed")
	}
	return bitmap
}

func (self *BackingFileHeader) WriteTo(writer io.Writer) (err error) {

	//magic number
	if _, err = writer.Write(MAGIC_NUMBER[:]); err != nil {
		return
	}

	//major version
	if err = binary.Write(writer, binary.BigEndian, self.MajorVersion); err != nil {
		return
	}

	//minor version
	if err = binary.Write(writer, binary.BigEndian, self.MinorVersion); err != nil {
		return
	}

	//uuid
	//fmt.Printf("%+v\n", self.UUID.Bytes())
	if _, err = writer.Write(self.UUID.Bytes()); err != nil {
		return
	}

	//RegionShift
	if err = binary.Write(writer, binary.BigEndian, self.RegionShift); err != nil {
		return
	}

	//BackingFile size = MaxCapacity + journalSize + 512;
	//MaxCapacity
	if err = binary.Write(writer, binary.BigEndian, self.MaxCapacity); err != nil {
		return
	}
	//单位是第几个32MB, 32MB * (1 << 32) = 128EB
	//header_size + journal_size = start of data
	if err = binary.Write(writer, binary.BigEndian, self.JournalSize); err != nil {
		return
	}
	return
}

//journal size = ceiling(MaxCapacity, RegionSize) / RegionSize  * ((crc(4) + tag(1) + start(4) + pos(4)) + (end tag)5) ceiling to 512
func fromMaxCapacityToJournalSize(maxCapacity uint64, regionShift uint16) uint64 {
	var regionSize uint64 = 1 << regionShift
	var numberOfRegion = (maxCapacity + regionSize - 1) / regionSize

	//(numberOfRegion + 1)
	//numberOfRegion of normal record
	//1 of end record

	//(4 + 1 + 4 + 4)
	//the first 4 byte is checksum
	//the second 1 byte is a tag which indicate if it is end
	//4 byte is for the number of 32MB in originFile
	//4 byte is for the number of 32MB from the start of dataRegion
	//the last 5 is the size of end tag
	//the start of dataRegion should be (512 + JournalSize)
	var sizeOfJouranl = (numberOfRegion)*(4+1+4+4) + 5

	//ceiling 512
	return (sizeOfJouranl + 511) / 512 * 512
}

func readBackingFileHeaderFrom(reader io.Reader) (header *BackingFileHeader, err error) {
	//only read the first sector
	reader = io.LimitReader(reader, 512)
	//read magic number;
	var magicNumber [4]byte
	if n, err := reader.Read(magicNumber[:]); err != nil {
		return nil, err
	} else if n != len(magicNumber) {
		return nil, errors.Wrap(internalerror.InvalidInput, "read magic number")

	} else if magicNumber != MAGIC_NUMBER {
		return nil, errors.Wrap(internalerror.InvalidInput, "read magic number")
	}
	//major version
	var majorVersion uint16
	if err := binary.Read(reader, binary.BigEndian, &majorVersion); err != nil {
		return nil, errors.Wrap(internalerror.InvalidInput, "read major vesion failed")
	} else if majorVersion != MAJOR_VERSION {
		return nil, errors.Wrapf(internalerror.InvalidInput, "read major verion not match: %v", majorVersion)
	}
	// minor version
	var minorVersion uint16
	if err := binary.Read(reader, binary.BigEndian, &minorVersion); err != nil {
		return nil, errors.Wrap(internalerror.InvalidInput, "read minor version failed")
	} else if minorVersion != MINOR_VERSION {
		return nil, errors.Wrapf(internalerror.InvalidInput, "read minor version not match:%v", minorVersion)
	}

	//uuid
	// UUID
	var uuidBuf [16]byte
	var fileUUID uuid.UUID
	if n, err := io.ReadFull(reader, uuidBuf[:]); err != nil {
		return nil, err
	} else if n != len(uuidBuf) {
		return nil, errors.Wrapf(internalerror.InvalidInput, "read uuid failed")
	}

	fileUUID, err = uuid.FromBytes(uuidBuf[:])
	if err != nil {
		return nil, internalerror.InvalidInput
	}

	//RegionShift
	var regionShift uint16
	if err := binary.Read(reader, binary.BigEndian, &regionShift); err != nil {
		return nil, errors.Wrapf(internalerror.InvalidInput, "read region shift failed")
	}
	//check regionShift
	if regionShift != 20 {
		return nil, errors.Wrapf(internalerror.InvalidInput, "check: regionShift is not 20")
	}

	//MaxCapacity
	var maxCapacity uint64
	if err := binary.Read(reader, binary.BigEndian, &maxCapacity); err != nil {
		return nil, errors.Wrapf(internalerror.InvalidInput, "read maxCapacity failed")
	}

	//journalSize
	var journalSize uint64
	if err := binary.Read(reader, binary.BigEndian, &journalSize); err != nil {
		return nil, errors.Wrapf(internalerror.InvalidInput, "read journalSize failed")
	}
	if journalSize != fromMaxCapacityToJournalSize(maxCapacity, regionShift) {
		return nil, errors.Wrapf(internalerror.InvalidInput, "check: journalSize failed")
	}

	return &BackingFileHeader{
		MajorVersion: majorVersion,
		MinorVersion: minorVersion,
		UUID:         fileUUID,
		RegionShift:  regionShift,
		MaxCapacity:  maxCapacity,
		JournalSize:  journalSize,
	}, nil
}

type BackingFile struct {
	file         *os.File
	JournalStart uint64
	JournalEnd   uint64
	dataStart    uint64
	dataEnd      uint64
	maxCapacity  uint64
	uid          uuid.UUID
	tree         judy.JudyL
	regionSize   uint64
}

func (bf *BackingFile) getFileName() string {
	return bf.uid.String() + "_lusf.snapshot"
}

func (bf *BackingFile) Close() {
	bf.file.Close()
}

//give [start, end) of the buf
//return an array which contains start of origin file's region which should be copyed later
//BUGGY: if end - start > 2 * RegionSize,
func (bf *BackingFile) GetCopyOffset(start uint64, end uint64) []uint64 {
	n0 := start / bf.regionSize
	n1 := (end - 1) / bf.regionSize
	var ret []uint64

	if n0 == n1 {
		if _, has := bf.tree.Get(n0); !has {
			ret = append(ret, n0*bf.regionSize)
			return ret
		}
		return ret
	}
	_, has0 := bf.tree.Get(n0)
	if !has0 {
		ret = append(ret, n0*bf.regionSize)
	}
	_, has1 := bf.tree.Get(n1)
	if !has1 {
		ret = append(ret, n1*bf.regionSize)
	}
	return ret
}

func (bf *BackingFile) FromOffsetToStart(offset uint64) uint64 {
	return offset / bf.regionSize
}

func (bf *BackingFile) RegionSize() uint64 {
	return bf.regionSize
}

//the size of buf should be the RegionSize,
//pos is the start of origin file
func (bf *BackingFile) Write(buf []byte, pos uint64) {
	if uint64(len(buf)) != bf.regionSize {
		panic("buf is small, i quit")
	}
	//pos is the
	start := bf.dataEnd
	if _, err := bf.file.Seek(int64(bf.dataEnd), io.SeekStart); err != nil {
		panic("im panic")
	}

	//most slowest part, write at least 32MB
	if _, err := bf.file.Write(buf); err != nil {
		panic("write panic")
	}

	bf.tree.Insert(pos/bf.regionSize, start)
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func CreateBackingFile(originFileSize uint64) (*BackingFile, error) {
	uuidFile := uuid.NewV4()
	fileName := uuidFile.String() + "_lusf.snapshot"
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	header := BackingFileHeader{
		MajorVersion: MAJOR_VERSION,
		MinorVersion: MINOR_VERSION,
		UUID:         uuidFile,
		RegionShift:  20, // 1<< 20, 32MB as default
		MaxCapacity:  originFileSize,
		JournalSize:  fromMaxCapacityToJournalSize(originFileSize, 20),
	}

	if err = header.WriteTo(file); err != nil {
		return nil, err
	}

	x := EndOfBitmap{}
	file.Seek(512, io.SeekStart)
	x.WriteTo(file)
	file.Sync()

	return &BackingFile{
		file:         file,
		JournalStart: 512,
		JournalEnd:   512,
		dataStart:    512 + header.JournalSize,
		dataEnd:      512 + header.JournalSize,
		maxCapacity:  header.MaxCapacity,
		tree:         judy.JudyL{},
		uid:          uuidFile,
		regionSize:   1 << header.RegionShift,
	}, nil
}

func OpenBackingFile(fileName string) (*BackingFile, error) {
	file, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	if err != nil {
		return nil, errors.Wrapf(internalerror.InvalidInput, "failed to open backfile %v", fileName)
	}
	var header *BackingFileHeader
	header, err = readBackingFileHeaderFrom(file)
	if err != nil {
		panic("asdf")
	}

	fmt.Printf("header is %+v\n", header)

	//read all the journalEntry to
	tree := judy.JudyL{}
	file.Seek(512, io.SeekStart)

	var dataStart uint64 = 512 + header.JournalSize //unit bytes
	var dataEnd = dataStart

	for {
		bitmap := ReadRecordFrom(file)
		if bitmap.Tag() == TAG_END_BITMAP {
			break
		}
		entry := bitmap.(BitmapEntry)
		entryDataEnd := dataStart + uint64(entry.SnapOffset+1)*(1<<header.RegionShift)
		dataEnd = util.Max(dataEnd, entryDataEnd)
		tree.Insert(uint64(entry.OriginOffset), uint64(entry.SnapOffset))
	}

	tail, err := file.Seek(0, os.SEEK_CUR)

	if uint64(tail) >= dataStart {
		panic("read failed")
	}

	//fmt.Printf(header.UUID.String())
	return &BackingFile{
		file:         file,
		JournalStart: 512, //fixed since create
		JournalEnd:   uint64(tail),
		dataStart:    dataStart,
		dataEnd:      dataEnd,
		maxCapacity:  header.MaxCapacity,
		tree:         tree,
		uid:          header.UUID,
		regionSize:   1 << header.RegionShift,
	}, nil
}

/*
	io.ReadWriteSeeker
	io.Closer
	Sync() error
	Position() uint64
	Capacity() uint64
	BlockSize() block.BlockSize
	Split(position uint64) (NonVolatileMemory, NonVolatileMemory, error)
	RawSize() int64
*/
type SnapNVM struct {
	originFile *nvm.FileNVM
	myBackfile *BackingFile
	ab         *block.AlignedBytes
	/*
		viewStart  uint64
		viewEnd    uint64
	*/
}

func (self *SnapNVM) Write(buf []byte) (n int, err error) {
	if len(buf) > (32 << 20) {
		panic("Write buf is too big")
	}
	start := self.originFile.Position() //start is a relative position
	realPos := self.originFile.ViewStart() + start
	if self.myBackfile != nil {
		copyVec := self.myBackfile.GetCopyOffset(realPos, realPos+uint64(len(buf)))
		for i := 0; i < len(copyVec); i++ {
			self.originFile.Seek(int64(copyVec[i]), io.SeekStart)
			self.ab.Resize(uint32(self.myBackfile.RegionSize()))
			//if we read too much data from originFile, the originFile will expand first, and then fill 0
			self.originFile.Read(self.ab.AsBytes())
			self.myBackfile.Write(self.ab.AsBytes(), copyVec[i])
		}
	}
	self.originFile.Seek(int64(start), io.SeekStart)
	return self.originFile.Write(buf)
}

func (self *SnapNVM) Read(buf []byte) (n int, err error) {
	return self.originFile.Read(buf)
}

//FIXME
func (self *SnapNVM) Seek(offset int64, whence int) (int64, error) {
	return self.originFile.Seek(offset, whence)
}

func (self *SnapNVM) Close() error {
	if self.myBackfile != nil {
		self.myBackfile.Close()
	}
	return self.originFile.Close()
}

func (self *SnapNVM) Split(position uint64) (sp1 nvm.NonVolatileMemory, sp2 nvm.NonVolatileMemory, err error) {
	left, right, _ := self.Split(position)

	sp1 = &SnapNVM{
		originFile: left.(*nvm.FileNVM),
		myBackfile: self.myBackfile,
		ab:         self.ab,
		/*
			viewStart:  self.originFile.ViewStart(),
			viewEnd:    self.originFile.ViewStart() + position,
		*/
	}

	sp2 = &SnapNVM{
		originFile: right.(*nvm.FileNVM),
		myBackfile: self.myBackfile,
		ab:         self.ab,
		/*
			viewStart:  self.originFile.ViewStart() + position,
			viewEnd:    self.originFile.ViewEnd(),
		*/
	}
	return sp1, sp2, nil
}

func (self *SnapNVM) BlockSize() block.BlockSize {
	return block.Min()
}

func (self *SnapNVM) Capacity() uint64 {
	return self.originFile.Capacity()
}

func (self *SnapNVM) Position() uint64 {
	return self.originFile.Position()
}

func (self *SnapNVM) RawSize() int64 {
	return self.originFile.RawSize() //FIXME, add backing file size
}

func (self *SnapNVM) Sync() error {
	return self.originFile.Sync()
}
