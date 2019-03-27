package lumpindex

import (
	"testing"

	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/portion"
)

var _ = fmt.Sprintf

func TestLumpIndexWork(t *testing.T) {
	l, _ := lump.FromString("1111")

	tree := NewIndex()
	data := portion.NewDataPortion(100, 10)
	tree.InsertDataPortion(l, data)

	d, err := tree.Get(l)

	assert.Nil(t, err)
	assert.Equal(t, data, d)

	l1, _ := lump.FromString("2222")
	data1 := portion.NewJournalPortion(100, 10)
	tree.InsertJournalPortion(l1, data1)
	d1, err := tree.Get(l1)
	assert.Nil(t, err)
	assert.Equal(t, data1, d1)

}

func TestLumpIndexDelete(t *testing.T) {
	cases := []lump.LumpId{
		lumpid("1111"),
		lumpid("2222"),
		lumpid("3333"),
		lumpid("4444"),
		lumpid("5555"),
	}
	tree := NewIndex()
	data := portion.NewJournalPortion(100, 10)
	for _, c := range cases {
		tree.InsertJournalPortion(c, data)
	}

	p, err := tree.Get(lumpid("4444"))
	assert.Nil(t, err)
	assert.Equal(t, data, p)

	//delete one entry
	tree.Delete(lumpid("4444"))
	p, err = tree.Get(lumpid("4444"))
	assert.Nil(t, p)
	assert.Error(t, err)

	//range delete
	tree.DeleteRange(lumpid("1111"), lumpid("5555"))

	for _, i := range cases[:4] {
		p, err = tree.Get(i)
		assert.Error(t, err)
	}
}

//helper

func lumpid(s string) lump.LumpId {
	l, _ := lump.FromString(s)
	return l
}
