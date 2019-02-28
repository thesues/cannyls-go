package nvm

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/block"
	"os"
	"testing"
)

func TestFileNVMOpen(t *testing.T) {

	//open an non-exist file
	nvm, err := Open("foo-test.lusf")
	assert.Error(t, err)
	assert.Nil(t, nvm)
}

func TestFileNVMCreate(t *testing.T) {
	//create a new file
	nvm, err := CreateIfAbsent("foo-test.lusf", 10*1024)
	assert.Nil(t, err)

	defer os.Remove("foo-test.lusf")

	data := bytes.NewBuffer([]byte{})
	err = DefaultStorageHeader().WriteTo(data)
	assert.Nil(t, err)

	data.Write([]byte("bar"))

	headerSize := data.Len()
	n, err := nvm.Write(align(data.Bytes()))

	assert.Equal(t, block.Min().CeilAlign(uint64(headerSize)), uint64(n))
	assert.Nil(t, err)
}

//helper function
func align(bytes []byte) []byte {
	ab := block.FromBytes(bytes, block.Min())
	return ab.Align().AsBytes()
}
