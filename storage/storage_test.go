package storage

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestCreateCannylsStorageCreateOpen(t *testing.T) {
	//10M
	_, err := CreateCannylsStorage("./tmp.lusf", 10<<20, 0.01)
	defer os.Remove("foo-test.lusf")
	assert.Nil(t, err)
}
