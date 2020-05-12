package nvm

import (
	_ "bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/util"
	judy "github.com/thesues/go-judy"

	"hash/adler32"

	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"github.com/thesues/cannyls-go/internalerror"
)

var (
	SNAP_MAGIC_NUMBER         = [4]byte{'s', 'n', 'a', 'p'}
	SNAP_MAJOR_VERSION uint16 = 0
	SNAP_MINOR_VERSION uint16 = 1
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
/*
const (
	TAG_END_BITMAP byte = 0
	TAG_BITMAP     byte = 1
)
*/

/*
type Bitmap interface {
	WriteTo(io.Writer) error
	CheckSum() uint32
	Tag() byte
}
*/

/*
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
*/

//the unit is fraction of "1 << RegionShift" , default is 32MB
type BitmapEntry struct {
	OriginOffset uint32
	SnapOffset   uint32
}

func (entry BitmapEntry) Size() uint64 {
	return 12
}
func (entry BitmapEntry) CheckSum() uint32 {
	hash := adler32.New()
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
	if err = binary.Write(writer, binary.BigEndian, entry.OriginOffset); err != nil {
		return
	}
	if err = binary.Write(writer, binary.BigEndian, entry.SnapOffset); err != nil {
		return
	}
	return
}

func ReadRecordFrom(reader io.Reader) BitmapEntry {
	var buf [4]byte
	if _, err := io.ReadFull(reader, buf[:]); err != nil {
		panic("can not read from disk: from ReadRecordFrom")
	}
	checksum := binary.BigEndian.Uint32(buf[:])
	var originOffset uint32
	var snapOffset uint32
	binary.Read(reader, binary.BigEndian, &originOffset)
	binary.Read(reader, binary.BigEndian, &snapOffset)
	bitmap := BitmapEntry{OriginOffset: originOffset, SnapOffset: snapOffset}
	//check entry's crc
	if bitmap.CheckSum() != checksum {
		panic("check sum failed")
	}
	return bitmap
}

func (self *BackingFileHeader) WriteTo(writer io.Writer) (err error) {

	//magic number
	if _, err = writer.Write(SNAP_MAGIC_NUMBER[:]); err != nil {
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
	//var sizeOfJouranl = (numberOfRegion)*(4+1+4+4) + 5

	var sizeOfJouranl = (numberOfRegion) * (4 + 4 + 4)
	//4 checksum
	//4 origin offset
	//4 snap offset

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

	} else if magicNumber != SNAP_MAGIC_NUMBER {
		return nil, errors.Wrap(internalerror.InvalidInput, "read magic number")
	}
	//major version
	var majorVersion uint16
	if err := binary.Read(reader, binary.BigEndian, &majorVersion); err != nil {
		return nil, errors.Wrap(internalerror.InvalidInput, "read major vesion failed")
	} else if majorVersion != SNAP_MAJOR_VERSION {
		return nil, errors.Wrapf(internalerror.InvalidInput, "read major verion not match: %v", majorVersion)
	}
	// minor version
	var minorVersion uint16
	if err := binary.Read(reader, binary.BigEndian, &minorVersion); err != nil {
		return nil, errors.Wrap(internalerror.InvalidInput, "read minor version failed")
	} else if minorVersion != SNAP_MINOR_VERSION {
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
	if regionShift < 25 {
		return nil, errors.Wrapf(internalerror.InvalidInput, "check: regionShift is less than 25")
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
	file           *os.File
	JournalStart   uint64
	JournalEnd     uint64
	journalMaxSize uint64
	dataStart      uint64
	dataEnd        uint64
	maxCapacity    uint64 //max_raw_size = 512 + journalMaxSize + maxCapacity
	uid            uuid.UUID
	tree           judy.JudyL
	regionSize     uint64
	fileName       string
}

/*
func (bf *BackingFile) getFileName() string {
	return bf.uid.String() + "_lusf.snapshot"
}
*/

func (bf *BackingFile) Close() {
	bf.file.Close()
}

func (bf *BackingFile) Sync() {
	bf.file.Sync()
}

//give [start, end) of the buf
//onBacking[] offset of backing file in the unit of byte
//onOrigin[] offset of origing file in the unit of byte
//all aligned to regionSize
func (bf *BackingFile) GetCopyOffset(start uint64, end uint64) (onBacking []uint32, onOrigin []uint32) {
	n0 := start / bf.regionSize
	n1 := (end - 1) / bf.regionSize
	for i := n0; i <= n1; i++ {
		if dataOffset, have := bf.tree.Get(i); have {
			//onBacking = append(onBacking, bf.dataStart+dataOffset*bf.regionSize)
			onBacking = append(onBacking, uint32(dataOffset))
		} else {
			onOrigin = append(onOrigin, uint32(i))
		}
	}
	return
}

func (bf *BackingFile) RegionSize() uint64 {
	return bf.regionSize
}

func (bf *BackingFile) GetFileName() string {
	return bf.fileName
}

//offset is the start of backing file
func (bf *BackingFile) ReadFromOffset(buf []byte, offset uint32) (int, error) {
	if uint64(len(buf)) != bf.regionSize {
		//panic("buf is small, i quit")
		panic(fmt.Sprintf("buf is small, i quit, regsionSize is %d, len of buf is %d\n", bf.regionSize, len(buf)))
	}

	var start uint64 = bf.dataStart + uint64(offset)*bf.regionSize
	if _, err := bf.file.Seek(int64(start), io.SeekStart); err != nil {
		panic(fmt.Sprintf("Seek panic pos: %d, %+v", start, err))
	}
	return io.ReadFull(bf.file, buf)
}

//WriteOffset would write buf into backing file.
//buf's size must be regionSize,
//offset is buf's offset in origin file
//WriteOffset write journal first, and then write the buf
func (bf *BackingFile) WriteOffset(buf []byte, offset uint32) {
	if uint64(len(buf)) != bf.regionSize {
		//panic("buf is small, i quit")
		panic(fmt.Sprintf("buf is small, i quit, regsionSize is %d, len of buf is %d\n", bf.regionSize, len(buf)))
	}
	//pos is the
	if _, have := bf.tree.Get(uint64(offset)); have {
		panic(fmt.Sprintf("offset is %d, we already snapshoted this", offset))
	}
	start := bf.dataEnd
	//TODO convert regionShift
	var entry BitmapEntry = BitmapEntry{
		OriginOffset: uint32(offset),
		SnapOffset:   uint32((start - bf.dataStart) / bf.regionSize),
	}
	//Write journal
	bf.file.Seek(int64(bf.JournalEnd), io.SeekStart)
	if err := entry.WriteTo(bf.file); err != nil {
		panic("write failed")
	}
	//Write data
	if _, err := bf.file.Seek(int64(bf.dataEnd), io.SeekStart); err != nil {
		panic("im panic")
	}
	bf.dataEnd += bf.regionSize
	if _, err := bf.file.Write(buf); err != nil {
		panic("write panic")
	}

	bf.JournalEnd += entry.Size()
	//fmt.Printf("entry insert %d:%d\n", entry.OriginOffset, entry.SnapOffset)
	bf.tree.Insert(uint64(entry.OriginOffset), uint64(entry.SnapOffset))
}

func CreateBackingFile(prefix string, originFileSize uint64) (*BackingFile, error) {
	uuidFile := uuid.NewV4()
	fileName := prefix + "_" + uuidFile.String() + "_lusf.snapshot"
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	header := BackingFileHeader{
		MajorVersion: SNAP_MAJOR_VERSION,
		MinorVersion: SNAP_MINOR_VERSION,
		UUID:         uuidFile,
		RegionShift:  25, // 1<< 20, 32MB as default
		MaxCapacity:  originFileSize,
		JournalSize:  fromMaxCapacityToJournalSize(originFileSize, 20),
	}

	if err = header.WriteTo(file); err != nil {
		return nil, err
	}

	file.Sync()

	return &BackingFile{
		file:           file,
		JournalStart:   512,
		JournalEnd:     512,
		dataStart:      512 + header.JournalSize,
		dataEnd:        512 + header.JournalSize,
		maxCapacity:    header.MaxCapacity,
		tree:           judy.JudyL{},
		uid:            uuidFile,
		regionSize:     1 << header.RegionShift,
		journalMaxSize: header.JournalSize,
		fileName:       fileName,
	}, nil
}

func OpenBackingFile(fileName string) (*BackingFile, error) {
	file, err := os.OpenFile(fileName, os.O_RDWR, 0755)
	if err != nil {
		return nil, errors.Wrapf(internalerror.InvalidInput, "failed to open backfile %v", fileName)
	}
	var header *BackingFileHeader
	header, err = readBackingFileHeaderFrom(file)
	if err != nil {
		panic(fmt.Sprint(err.Error()))
	}

	fmt.Printf("header is %+v\n", header)

	//read all the journalEntry to
	tree := judy.JudyL{}

	var dataStart uint64 = 512 + header.JournalSize //unit bytes
	info, _ := file.Stat()
	var dataEnd = util.Max(dataStart, uint64(info.Size()))

	n := (dataEnd - dataStart) >> header.RegionShift

	//fmt.Printf("snapshot number of entry is %d\n", n)

	if _, err = file.Seek(512, io.SeekStart); err != nil {
		panic("seek failed")
	}
	for i := uint64(0); i < n; i++ {
		entry := ReadRecordFrom(file)
		entryDataEnd := dataStart + uint64(entry.SnapOffset+1)*(1<<header.RegionShift)
		dataEnd = util.Max(dataEnd, entryDataEnd)
		//fmt.Printf("entry %d %d\n", entry.OriginOffset, entry.SnapOffset)
		tree.Insert(uint64(entry.OriginOffset), uint64(entry.SnapOffset))
	}
	journalEnd := 512 + 12*n

	//fmt.Printf(header.UUID.String())
	return &BackingFile{
		file:           file,
		JournalStart:   512, //fixed since create
		JournalEnd:     journalEnd,
		dataStart:      dataStart,
		dataEnd:        dataEnd,
		maxCapacity:    header.MaxCapacity,
		tree:           tree,
		uid:            header.UUID,
		regionSize:     1 << header.RegionShift,
		journalMaxSize: header.JournalSize,
	}, nil
}

type SnapshotReader struct {
	snap          *SnapNVM
	offset        uint64
	buf           *block.AlignedBytes
	waterHighMark int
	waterLowMark  int
}

func newSnapshotReader(snap *SnapNVM) *SnapshotReader {
	ab := block.NewAlignedBytes(int(snap.myBackfile.RegionSize()), block.Min())
	return &SnapshotReader{
		snap:          snap,
		buf:           ab,
		offset:        0,
		waterHighMark: 0,
		waterLowMark:  0,
	}
}

func (self *SnapshotReader) Seek(offset int64, whence int) (int64, error) {
	self.waterHighMark = 0
	self.waterLowMark = 0
	switch whence {
	case 0:
		self.offset = uint64(offset)
		return offset, nil
	case 1:
		self.offset = uint64(int64(self.offset) + offset)
		return int64(self.offset), nil
	case 2:
		panic("not supported so far")
	default:
		return 0, errors.New("failed to seek")
	}

}
func (self *SnapshotReader) Read(p []byte) (n int, err error) {

	if self.snap.originFile != self.snap.rawFile {
		panic("for reader, rawFile == originFile")
	}
	regionSize := self.snap.myBackfile.RegionSize()
	/* local buffer is empty */
	if uint64(len(p)) > self.snap.myBackfile.RegionSize() {
		panic("Read data too big")
	}
	start := self.offset / regionSize * regionSize
	//read the whole region size
	//in this implement, self.waterHighMark will always be (32<<20)
	if self.waterHighMark == self.waterLowMark {
		//readSize := (self.offset+regionSize)/regionSize*regionSize - self.offset
		//onBacking, onOrigin := self.snap.myBackfile.GetCopyOffset(self.offset, self.offset+readSize)
		onBacking, onOrigin := self.snap.myBackfile.GetCopyOffset(start, start+regionSize)
		fmt.Printf("onBacking is %+v, onOrigin is %+v, offset is %d, start is %d\n", onBacking, onOrigin, self.offset, start)
		if len(onBacking) == 1 {
			n, err = self.snap.myBackfile.ReadFromOffset(self.buf.AsBytes(), onBacking[0])
			if n < 0 {
				panic("size is not good")
			}
			if n == 0 {
				return 0, io.EOF
			}
		} else if len(onOrigin) == 1 {
			if _, err = self.snap.originFile.Seek(int64(uint64(onOrigin[0])*regionSize), io.SeekStart); err != nil {
				panic("failed to seek")
			}
			//fmt.Printf("read full from %d, len: %d\n", uint64(onOrigin[0])*regionSize, len(self.buf.AsBytes()))
			n, err = io.ReadFull(self.snap.originFile, self.buf.AsBytes())
			//n, err = self.snap.originFile.Read(self.buf.AsBytes())
			if n < 0 {
				panic("size is not good")
			}
			if n == 0 {
				return 0, io.EOF
			}
		} else {
			panic("bad happend")
		}
		self.waterHighMark = n
		self.waterLowMark = int(self.offset % self.snap.myBackfile.RegionSize())
		self.offset += uint64(n)

	}
	//fmt.Printf("l is %d, n is %d, hi is %d, lo is %d\n", len(p), n, self.waterHighMark, self.waterLowMark)
	l := len(p)
	if l < self.waterHighMark-self.waterLowMark {
		copy(p, self.buf.AsBytes()[self.waterLowMark:self.waterLowMark+l])
		self.waterLowMark += l
		return l, nil
	} else {
		copy(p, self.buf.AsBytes()[self.waterLowMark:])
		return self.waterHighMark - self.waterLowMark, nil
	}
}

type SnapNVM struct {
	originFile *FileNVM
	myBackfile *BackingFile
	rawFile    *FileNVM
	ab         *block.AlignedBytes
	reader     *SnapshotReader
	prefix     string
	splited    bool
}

func NewSnapshotNVM(originFile *FileNVM) (*SnapNVM, error) {

	if originFile.splited {
		panic("can not create snap from splited NVM")
	}
	var err error
	fileDirectory := filepath.Dir(originFile.path)
	var myBackFile *BackingFile = nil
	///a/b/c/d/EXXX.lusf => EXXX
	n := len(originFile.path)
	prefix := filepath.Base(originFile.path)[0 : n-5]
	pattern := fmt.Sprintf("%s/%s_*_lusf.snapshot", fileDirectory, prefix)
	//fmt.Println(pattern)
	matches, err := filepath.Glob(pattern)
	fmt.Println(matches)
	if err != nil {
		panic(err.Error())
	}
	if len(matches) == 0 {
		//myBackFile, err = CreateBackingFile(prefix, uint64(originFile.RawSize()))
	} else if len(matches) == 1 {
		myBackFile, err = OpenBackingFile(matches[0])
	}
	if err != nil {
		return nil, err
	}
	snapNVM := &SnapNVM{
		originFile: originFile,
		myBackfile: myBackFile,
		prefix:     prefix,
		rawFile:    originFile,
		splited:    false,
	}
	if myBackFile != nil {
		regionSize := myBackFile.RegionSize()
		snapNVM.reader = newSnapshotReader(snapNVM)
		snapNVM.ab = block.NewAlignedBytes(int(regionSize), block.Min())
	}
	return snapNVM, nil
}

func (self *SnapNVM) Write(buf []byte) (n int, err error) {
	if len(buf) > (32 << 20) {
		panic("Write buf is too big")
	}

	start := self.originFile.Position() //start is a relative position
	realPos := self.originFile.ViewStart() + start
	if self.myBackfile != nil {
		_, onOrigin := self.myBackfile.GetCopyOffset(realPos, realPos+uint64(len(buf)))
		fmt.Printf("start backup for %+v, start is %d , realPos is start:end is %d:%d \n",
			onOrigin, start, realPos, realPos+uint64(len(buf)))
		for i := 0; i < len(onOrigin); i++ {
			//FIXMEFUCK
			self.rawFile.Seek(int64(uint64(onOrigin[i])*self.myBackfile.RegionSize()), io.SeekStart)

			//fmt.Printf("offset is %d, len is %d\n", uint64(onOrigin[i])*self.myBackfile.RegionSize(), self.ab.Len())
			n, err = io.ReadFull(self.rawFile, self.ab.AsBytes())
			if n < 0 {
				panic("fail to read data")
			}
			//n, err = self.originFile.Read(self.ab.AsBytes())
			//fmt.Printf("n %d, err %+v\n", n, err)

			self.myBackfile.WriteOffset(self.ab.AsBytes(), onOrigin[i])
			//fmt.Printf("%d copied to backing file\n", onOrigin[i])
			//a, b := self.myBackfile.GetCopyOffset((32 << 20), (32<<20)+1)
			//fmt.Printf("issue: onbacking is %+v, onorigin is %+v\n", a, b)
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
		self.myBackfile.Sync()
		self.myBackfile.Close()
	}
	return self.originFile.Close()
}

func (self *SnapNVM) Split(position uint64) (sp1 NonVolatileMemory, sp2 NonVolatileMemory, err error) {
	left, right, _ := self.originFile.Split(position)

	sp1 = &SnapNVM{
		originFile: left.(*FileNVM),
		myBackfile: self.myBackfile,
		ab:         self.ab,
		rawFile:    self.rawFile,
		splited:    true,
		/*
			viewStart:  self.originFile.ViewStart(),
			viewEnd:    self.originFile.ViewStart() + position,
		*/
	}

	sp2 = &SnapNVM{
		originFile: right.(*FileNVM),
		myBackfile: self.myBackfile,
		ab:         self.ab,
		rawFile:    self.rawFile,
		splited:    true,
		/*
			viewStart:  self.originFile.ViewStart() + position,
			viewEnd:    self.originFile.ViewEnd(),
		*/
	}
	return sp1, sp2, nil
}

func (self *SnapNVM) CreateSnapshotIfNeeded() (reader *SnapshotReader, err error) {
	if self.myBackfile != nil {
		return self.reader, nil
	}
	self.myBackfile, err = CreateBackingFile(self.prefix, uint64(self.RawSize()))
	regionSize := self.myBackfile.RegionSize()
	self.ab = block.NewAlignedBytes(int(regionSize), block.Min())
	if err != nil {
		return
	}
	self.reader = newSnapshotReader(self)
	return self.reader, nil
}

//return SnapShotReader

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
