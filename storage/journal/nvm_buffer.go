package journal

import (
	"fmt"
	"io"

	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/util"
)

var _ = fmt.Println

type JournalNvmBuffer struct {
	nvm            nvm.NonVolatileMemory
	position       uint64
	writeBuf       *block.AlignedBytes
	readBuf        *block.AlignedBytes
	writeBufOffset uint64
	maybeDirty     bool
}

func NewJournalNvmBuffer(nvm nvm.NonVolatileMemory) *JournalNvmBuffer {
	bsize := nvm.BlockSize()
	return &JournalNvmBuffer{
		nvm:            nvm,
		position:       0,
		writeBuf:       block.NewAlignedBytes(0, bsize),
		readBuf:        block.NewAlignedBytes(0, bsize),
		writeBufOffset: 0,
		maybeDirty:     false,
	}

}

//implement NonVolatileNVM interface
func (jb *JournalNvmBuffer) Sync() error {
	if err := jb.flushWriteBuffer(); err != nil {
		return err
	}
	return jb.nvm.Sync()
}

//
func (jb *JournalNvmBuffer) Position() uint64 {
	return jb.position
}

func (jb *JournalNvmBuffer) Split(p uint64) (nvm.NonVolatileMemory, nvm.NonVolatileMemory, error) {
	panic("Journal Buffer should not call Split!")
}

func (jb *JournalNvmBuffer) Seek(offset int64, whence int) (int64, error) {
	abs, err := nvm.ConvertToOffset(jb, offset, whence)
	if err != nil {
		return -1, err
	}
	jb.position = uint64(abs)
	return abs, nil

}

//Direct Read
func (jb *JournalNvmBuffer) Read(buf []byte) (n int, err error) {
	if jb.isDirty(jb.position, len(buf)) {
		//trigger flush
		if err := jb.flushWriteBuffer(); err != nil {
			return 0, err
		}
	}

	readBufStart := jb.nvm.BlockSize().FloorAlign(jb.position)
	readBufEnd := jb.nvm.BlockSize().CeilAlign(jb.position + uint64(len(buf)))
	jb.readBuf.AlignResize(uint32(readBufEnd - readBufStart))

	//fmt.Printf("len: ", jb.readBuf.Len())
	//Seek the aligned sector and read from disk
	if _, err := jb.nvm.Seek(int64(readBufStart), io.SeekStart); err != nil {
		return -1, err
	}

	innerReadSize, err := jb.nvm.Read(jb.readBuf.AsBytes())
	if err != nil {
		return -1, err
	}

	start := jb.position - readBufStart
	end := util.Min(uint64(innerReadSize), start+uint64(len(buf)))
	readSize := end - start

	//fmt.Printf("inner read size: %d,buf len %d, readSize %d, start: %d, end :%d\n", innerReadSize, len(buf), readSize, start, end)
	copy(buf[:readSize], jb.readBuf.AsBytes()[start:end])
	jb.position += readSize
	return int(readSize), nil

}

//Writeback
func (jb *JournalNvmBuffer) Write(buf []byte) (n int, err error) {
	if jb.isOverflow(uint32(len(buf))) {
		return 0, internalerror.InconsistentState
	}

	writeBufStart := jb.writeBufOffset
	writeBufEnd := jb.writeBufOffset + uint64(jb.writeBuf.Len())

	//User write in the write buffer
	if writeBufStart <= jb.position && jb.position <= writeBufEnd {
		//start, end is relative to the start of write buffer
		//fmt.Println("here1")
		start := jb.position - writeBufStart
		end := uint32(start) + uint32(len(buf))
		jb.writeBuf.AlignResize(end)
		copy(jb.writeBuf.AsBytes()[start:end], buf)
		jb.position += uint64(len(buf))
		jb.maybeDirty = true
		//fmt.Printf("buffer size is %d\n", jb.writeBuf.Len())
		return len(buf), nil
	} else {
		if err := jb.flushWriteBuffer(); err != nil {
			return 0, err
		}
		//prepare new buffer
		//try to call Write again, this time, the newly created buf would be used
		if jb.nvm.BlockSize().IsAligned(jb.position) {
			//fmt.Println("here2")
			jb.writeBuf.AlignResize(0)
			jb.writeBufOffset = jb.position
			//fmt.Printf("update buf offset to %d", jb.position)
		} else {
			//fmt.Println("here3")
			jb.writeBufOffset = jb.nvm.BlockSize().FloorAlign(jb.position)
			//fmt.Printf("update buf offset to %d", jb.position)
			jb.writeBuf.AlignResize(uint32(jb.nvm.BlockSize().AsU16())) //resize to a sector
			jb.nvm.Seek(int64(jb.writeBufOffset), io.SeekStart)
			jb.nvm.Read(jb.writeBuf.AsBytes())
		}

		//call
		return jb.Write(buf)
	}
}

func (jb *JournalNvmBuffer) Close() error {
	return jb.Sync()
}

func (jb *JournalNvmBuffer) BlockSize() block.BlockSize {
	return jb.nvm.BlockSize()
}

func (jb *JournalNvmBuffer) Flush() error {
	return jb.flushWriteBuffer()

}

func (jb *JournalNvmBuffer) flushWriteBuffer() error {
	if jb.writeBuf.Len() == 0 || jb.maybeDirty == false {
		return nil
	}

	//fmt.Println("FLUSH DATA")
	if _, err := jb.nvm.Seek(int64(jb.writeBufOffset), io.SeekStart); err != nil {
		return err
	}
	if _, err := jb.nvm.Write(jb.writeBuf.AsBytes()); err != nil {
		return err
	}

	//Save the last sector to prevent from reread if write buf is bigger than one sector
	if jb.writeBuf.Len() > uint32(jb.BlockSize().AsU16()) {
		newLen := uint32(jb.BlockSize().AsU16()) //a sector
		dropLen := jb.writeBuf.Len() - newLen

		//Move the last sector
		copy(jb.writeBuf.AsBytes(), jb.writeBuf.AsBytes()[dropLen:])

		jb.writeBuf.Truncate(newLen)
		jb.writeBufOffset += uint64(dropLen)
	}
	jb.maybeDirty = false
	return nil
}

func (jb *JournalNvmBuffer) Capacity() uint64 {
	return jb.nvm.Capacity()
}

func (jb *JournalNvmBuffer) isOverflow(len uint32) bool {
	if jb.position+uint64(len) > jb.Capacity() {
		return true
	}
	return false
}

func (jb *JournalNvmBuffer) isDirty(offset uint64, length int) bool {
	if !jb.maybeDirty || length == 0 || jb.writeBuf.Len() == 0 {
		return false
	}

	// write buf has overlap?
	if jb.writeBufOffset < offset {
		//offset is in the writeBuf?
		writeBufEnd := jb.writeBufOffset + uint64(jb.writeBuf.Len())
		return offset < writeBufEnd
	} else {
		//end in the writeBuf?
		end := offset + uint64(length)
		return jb.writeBufOffset < end
	}

}
