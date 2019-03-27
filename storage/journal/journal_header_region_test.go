package journal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/nvm"
)

func TestJournalHeaderRegion(t *testing.T) {
	f, _ := nvm.New(1024)
	region := NewJournalHeadRegion(f)
	region.WriteTo(1234)

	head, err := region.ReadFrom()
	assert.Nil(t, err)
	assert.Equal(t, uint64(1234), head)

}
