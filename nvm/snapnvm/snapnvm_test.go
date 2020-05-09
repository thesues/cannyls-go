package snapnvm

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackingFileCreate(t *testing.T) {
	f, err := CreateBackingFile(10 << 20)
	assert.Nil(t, err)
	defer os.Remove(f.getFileName())
}

func TestBackingFileOpen(t *testing.T) {
	f, err := CreateBackingFile(10 << 20)
	//defer os.Remove(f.getFileName())
	assert.Nil(t, err)
	fileName := f.getFileName()
	f.Close()

	fmt.Printf("filename is %s\n", fileName)
	f, err = OpenBackingFile(fileName)
	assert.Nil(t, err)
	fmt.Printf("%+v\n", f)
}
