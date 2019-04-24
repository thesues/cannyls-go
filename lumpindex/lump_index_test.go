package lumpindex

import (
	"testing"

	"fmt"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/portion"
	"runtime"
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

const N int64 = 10000000

func TestSpace(t *testing.T) {
	var msb, msa runtime.MemStats
	runtime.ReadMemStats(&msb)

	tree := NewIndex()
	var i uint64
	for i = 0; i < uint64(N); i++ {
		id := lump.FromU64(0, i)
		tree.InsertDataPortion(id, portion.NewDataPortion(64, 64))
	}

	p := tree.DataPortions()

	fmt.Println(p[N/2])

	runtime.ReadMemStats(&msa)
	fmt.Printf("LUMP Index %0.0f MiB allocated for %s\n", float64(msa.Alloc-msb.Alloc)/float64(1<<20), humanize.Comma(N))
}

func lumpid(s string) lump.LumpId {
	l, _ := lump.FromString(s)
	return l
}
