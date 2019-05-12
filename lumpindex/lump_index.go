package lumpindex

import (
	"fmt"
	"math"

	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/portion"
	judy "github.com/thesues/go-judy"
)

var _ = fmt.Println

type LumpIndex struct {
	tree judy.JudyL
}

func NewIndex() *LumpIndex {
	tree := judy.JudyL{}
	return &LumpIndex{
		tree: tree,
	}
}

func (index *LumpIndex) Get(id lump.LumpId) (p portion.Portion, err error) {
	v, ok := index.tree.Get(id.U64())
	if ok == false {
		return nil, errors.Wrapf(internalerror.InvalidInput, "failed to get key :%s", id.String())
	}

	p, _ = fromValueToPortion(v)
	return
}

func (index *LumpIndex) InsertDataPortion(id lump.LumpId, data portion.DataPortion) {
	var n uint64 = 0
	n = data.Start.AsU64() | uint64(data.Len)<<40 | 1<<63
	index.tree.Insert(id.U64(), n)
}

func (index *LumpIndex) InsertJournalPortion(id lump.LumpId, data portion.JournalPortion) {
	var n uint64 = 0
	n = data.Start.AsU64() | uint64(data.Len)<<40
	index.tree.Insert(id.U64(), n)
}

func (index *LumpIndex) Delete(id lump.LumpId) bool {
	return index.tree.Delete(id.U64())
}

func (index *LumpIndex) DeleteRange(start lump.LumpId, end lump.LumpId) {
	indexNum, _, ok := index.tree.First(start.U64())
	for ok && indexNum < end.U64() {
		if rc := index.tree.Delete(indexNum); rc == false {
			fmt.Printf("index %d\n", indexNum)
			panic("judy index, delete item when iterating.. should never happen")
		}
		indexNum, _, ok = index.tree.Next(indexNum)
	}
}

func (index *LumpIndex) Min() (id lump.LumpId, ok bool) {
	var n uint64
	n, _, ok = index.tree.First(0)
	if ok {
		id = lump.FromU64(0, n)
	}
	return
}

func (index *LumpIndex) Max() (id lump.LumpId, ok bool) {
	var n uint64
	ok = false
	n, _, ok = index.tree.Last(math.MaxUint64)
	if ok {
		id = lump.FromU64(0, n)
	}
	return
}

func (index *LumpIndex) FirstEmpty() (id lump.LumpId, ok bool) {
	ok = false
	n, ok := index.tree.FirstEmpty(0)
	if ok {
		id = lump.FromU64(0, n)
	}
	return
}

func (index *LumpIndex) List() []lump.LumpId {
	vec := make([]lump.LumpId, 0, 1024)
	indexNum, _, ok := index.tree.First(0)
	for ok {
		vec = append(vec, lump.FromU64(0, indexNum))
		indexNum, _, ok = index.tree.Next(indexNum)
	}
	return vec
}
func (index *LumpIndex) DataPortions() []portion.DataPortion {
	n := index.tree.CountAll()
	vec := make([]portion.DataPortion, 1, 100000+n)
	vec[0] = portion.NewDataPortion(0, 0)

	indexNum, value, ok := index.tree.First(0)

	for ok {
		if p, isDataPortion := fromValueToPortion(value); isDataPortion {
			vec = append(vec, p.(portion.DataPortion))
		}
		indexNum, value, ok = index.tree.Next(indexNum)
	}
	return vec
}

func (index *LumpIndex) ListRange(start lump.LumpId, end lump.LumpId) []lump.LumpId {
	vec := make([]lump.LumpId, 0, 1024)
	indexNum, _, ok := index.tree.First(start.U64())
	for ok && indexNum < end.U64() {
		vec = append(vec, lump.FromU64(0, indexNum))
		indexNum, _, ok = index.tree.Next(indexNum)
	}
	return vec
}

func (index *LumpIndex) MemoryUsed() uint64 {
	return index.tree.MemoryUsed()
}

func fromValueToPortion(value uint64) (p portion.Portion, isDataPortion bool) {
	n := value
	//data portion
	kind := n >> 63
	len := uint16(n >> 40 & 0xFFFF)
	start := n & (address.MAX_ADDRESS)
	if kind == 0 {
		p = portion.NewJournalPortion(start, len)
		isDataPortion = false
	} else {
		p = portion.NewDataPortion(start, len)
		isDataPortion = true
	}
	return
}
