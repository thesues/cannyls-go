package nvm

import (
	_ "bytes"
	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/util"
	"io"
	"os"
)

type FileNVM struct {
	file            *os.File
	cursor_position uint64
	view_start      uint64
	view_end        uint64
	splited         bool //splited file is not allowd to call file.Close()
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func CreateIfAbsent(path string, capacity uint64) (*FileNVM, error) {

	if block.Min().IsAligned(capacity) == false {
		return nil, internalerror.InvalidInput
	}

	if fileExists(path) {
		return nil, os.ErrExist
	}
	var flags int
	var f *os.File
	var err error
	flags = os.O_CREATE | os.O_RDWR

	if f, err = openFileWithDirectIO(path, flags, 0755); err != nil {
		return nil, errors.Wrapf(err, "failed to open file %s\n", path)
	}
	/*
		if f, err = os.OpenFile(path, flags, 0755); err != nil {
			return nil, errors.Wrapf(err, "failed to open file %s\n", path)
		}
	*/

	/*
		var metadata os.FileInfo
		if metadata, err = f.Stat(); err != nil {
			return nil, errors.Wrap(err, "failed to get metadata")
		}

			if metadata.Size() == 0 {
				//TODO prealloc
			} else {
				//aligned read
				var buf [512]byte
				if _, err = f.Read(buf[:]); err != nil {
					return nil, err
				}
				meta := bytes.NewReader(buf[:])
				header, err := ReadFrom(meta)
				if err != nil {
					return nil, err
				}
				capacity = header.StorageSize()
			}
	*/

	if err = lockFileWithExclusiveLock(f); err != nil {
		return nil, err
	}

	return &FileNVM{
		file:            f,
		cursor_position: 0,
		view_start:      0,
		view_end:        capacity,
		splited:         false,
	}, nil

}

func Open(path string) (nvm *FileNVM, header *StorageHeader, err error) {
	var f, parsedFile *os.File

	if parsedFile, err = os.OpenFile(path, os.O_RDWR, 07555); err != nil {
		return nil, nil, err
	}
	//read the first sector
	if header, err = ReadFromFile(parsedFile); err != nil {
		parsedFile.Close()
		return nil, nil, err
	}

	capacity := header.StorageSize()

	//reopen the file
	parsedFile.Close()
	if f, err = openFileWithDirectIO(path, os.O_RDWR, 0755); err != nil {
		return nil, nil, err
	}

	if err = lockFileWithExclusiveLock(f); err != nil {
		return nil, nil, err
	}
	err = nil
	nvm = &FileNVM{
		file:            f,
		cursor_position: 0,
		view_start:      0,
		view_end:        capacity,
		splited:         false,
	}
	return
}

func (self *FileNVM) Sync() error {
	return self.file.Sync()
}

func (self *FileNVM) Position() uint64 {
	return self.cursor_position - self.view_start
}

func (nvm *FileNVM) Capacity() uint64 {
	return nvm.view_end - nvm.view_start
}

func (nvm *FileNVM) Split(position uint64) (sp1 NonVolatileMemory, sp2 NonVolatileMemory, err error) {
	if block.Min().CeilAlign(uint64(position)) != position {
		return nil, nil, errors.Wrapf(internalerror.InvalidInput, "not aligned :%d in split", position)
	}

	//TODO
	leftNVM := &FileNVM{
		file:            nvm.file,
		view_start:      nvm.view_start,
		cursor_position: nvm.view_start,
		view_end:        nvm.view_start + position,
		splited:         true,
	}

	rightNVM := &FileNVM{
		file:            nvm.file,
		view_start:      leftNVM.view_end,
		view_end:        nvm.view_end,
		cursor_position: leftNVM.view_end,
		splited:         true,
	}

	return leftNVM, rightNVM, nil
}

func (nvm *FileNVM) Seek(offset int64, whence int) (int64, error) {
	if !block.Min().IsAligned(uint64(offset)) {
		return offset, errors.Wrapf(internalerror.InvalidInput, "not aligned :%d in seek", offset)
	}

	//abs is relative to the current FileNVM, start from 0
	abs, err := ConvertToOffset(nvm, offset, whence)
	if err != nil {
		return 0, err
	}

	if abs > int64(nvm.Capacity()) || abs < 0 {
		return -1, errors.Wrapf(internalerror.InvalidInput, "seek abs is wrong %d in seek", abs)
	}

	realFilePosition := nvm.view_start + uint64(abs)

	nvm.cursor_position = realFilePosition
	return offset, nil
}

func (nvm *FileNVM) Read(buf []byte) (n int, err error) {
	maxLen := nvm.Capacity() - nvm.Position()
	bufLen := uint64(len(buf))
	if !block.Min().IsAligned(uint64(bufLen)) {
		return -1, errors.Wrapf(internalerror.InvalidInput, "not aligned :%d, in read", bufLen)
	}

	len := util.Min(maxLen, bufLen)

	newPosition := nvm.cursor_position + len

	n, err = nvm.file.ReadAt(buf[:len], int64(nvm.cursor_position))
	if err == io.EOF {
		//expand the file
		nvm.file.Seek(int64(newPosition), io.SeekStart)
		nvm.cursor_position = newPosition
		return int(len), nil
	}
	if err != nil {
		return -1, errors.Wrap(err, "FileNVM failed to read")
	}
	if n < int(len) {
		//if uint64(n) < len {
		//expand the file
		nvm.file.Seek(int64(newPosition), io.SeekStart)
		nvm.cursor_position = newPosition
		return int(len), nil
	}

	nvm.cursor_position = newPosition
	return n, nil
}

func (nvm *FileNVM) Write(buf []byte) (n int, err error) {
	maxLen := nvm.Capacity() - nvm.Position()
	bufLen := uint64(len(buf))

	if !block.Min().IsAligned(uint64(bufLen)) {
		return -1, errors.Wrapf(internalerror.InvalidInput, "not aligned :%d, in write", bufLen)
	}

	len := util.Min(maxLen, bufLen)
	newPosition := nvm.cursor_position + len

	if n, err = nvm.file.WriteAt(buf[:len], int64(nvm.cursor_position)); err != nil {
		return -1, errors.Wrap(err, "FileNVM failed to write")
	}

	nvm.cursor_position = newPosition

	return n, nil

}

func (nvm *FileNVM) Close() error {
	if !nvm.splited {
		return nvm.file.Close()
	} else {
		return nil
	}
}

func (nvm *FileNVM) BlockSize() block.BlockSize {
	return block.Min()
}
