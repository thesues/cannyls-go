package nvm

import (
	"testing"

	"fmt"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
	"io"
	"io/ioutil"
	"os"
)

func TestStorageHeader(t *testing.T) {

	bs, err := block.NewBlockSize(512)
	assert.Nil(t, err)

	uuid, err := uuid.NewV4()
	assert.Nil(t, err)

	header := StorageHeader{
		MajorVersion:      MAJOR_VERSION,
		MinorVersion:      MINOR_VERSION,
		BlockSize:         bs,
		UUID:              uuid,
		JournalRegionSize: 1024,
		DataRegionSize:    4096,
	}

	assert.Equal(t, header.RegionSize(), uint64(512))
	assert.Equal(t, header.StorageSize(), uint64(512+1024+4096))

	//read/write
	tempfile, err := ioutil.TempFile("", "example")
	assert.Equal(t, nil, err)
	defer os.Remove(tempfile.Name())

	err = header.WriteHeaderRegionTo(tempfile)
	assert.Equal(t, nil, err)

	tempfile.Seek(0, io.SeekStart)
	var otherHeader *StorageHeader

	otherHeader, err = ReadFrom(tempfile)

	assert.Nil(t, err)

	if err != nil {
		fmt.Printf("# Extended value:\n%+v\n\n", err)

	}
	fmt.Printf("%+v\n", otherHeader)
	assert.Equal(t, header.MajorVersion, otherHeader.MajorVersion)
	assert.Equal(t, header.MinorVersion, otherHeader.MinorVersion)
	assert.Equal(t, header.BlockSize, otherHeader.BlockSize)
	assert.Equal(t, header.UUID, otherHeader.UUID)
	assert.Equal(t, header.JournalRegionSize, otherHeader.JournalRegionSize)
	assert.Equal(t, header.DataRegionSize, otherHeader.DataRegionSize)

}
