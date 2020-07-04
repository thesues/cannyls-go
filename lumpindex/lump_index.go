package lumpindex

import (
	"fmt"
	"math"

	"sync/atomic"

	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/portion"
	judy "github.com/thesues/go-judy"
)

var _ = fmt.Println

type LumpIndex struct {
	tree   judy.JudyL
	counts uint64
}

func NewIndex() *LumpIndex {
	tree := judy.JudyL{}
	return &LumpIndex{
		tree:   tree,
		counts: 0,
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
	atomic.AddUint64(&index.counts, 1)
}

func (index *LumpIndex) InsertJournalPortion(id lump.LumpId, data portion.JournalPortion) {
	var n uint64 = 0
	n = data.Start.AsU64() | uint64(data.Len)<<40
	index.tree.Insert(id.U64(), n)
	atomic.AddUint64(&index.counts, 1)
}

func (index *LumpIndex) Delete(id lump.LumpId) bool {
	atomic.AddUint64(&index.counts, ^uint64(0))
	return index.tree.Delete(id.U64())
}

func (index *LumpIndex) DeleteRange(start lump.LumpId, end lump.LumpId) {
	indexNum, _, ok := index.tree.First(start.U64())
	n := 0
	for ok && indexNum < end.U64() {
		if rc := index.tree.Delete(indexNum); rc == false {
			fmt.Printf("index %d\n", indexNum)
			panic("judy index, delete item when iterating.. should never happen")
		}
		indexNum, _, ok = index.tree.Next(indexNum)
		n += 1
	}
	atomic.AddUint64(&index.counts, ^uint64(n-1))
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

func (index *LumpIndex) Free() {
	index.tree.Free()
}
func (index *LumpIndex) FirstEmpty() (id lump.LumpId, ok bool) {
	ok = false
	n, ok := index.tree.FirstEmpty(0)
	if ok {
		id = lump.FromU64(0, n)
	}
	return
}

//thread safe
func (index *LumpIndex) Count() uint64 {
	return atomic.LoadUint64(&index.counts)
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

//WARNING: this function returns judy.Judy1, It can not be released by go gc.
//the developer is responsible to free it
//judyPortionArray is ordered by start of each portion
func (index *LumpIndex) JudyDataPortions() *judy.Judy1 {
	judyPortionArray := judy.Judy1{}
	var judyPoriton uint64

	judyPortionArray.Set(0)
	indexNum, value, ok := index.tree.First(0)
	for ok {
		if p, isDataPortion := fromValueToPortion(value); isDataPortion {
			judyPoriton = fromDataPortionToJudy(p.(portion.DataPortion))
			judyPortionArray.Set(judyPoriton)
		}
		indexNum, value, ok = index.tree.Next(indexNum)
	}
	return &judyPortionArray
}

/*
Loop all the index, if it's a Dataportion(not a Journalportion), It must occupy
some part of the disk, Append this DataPortion to a
*/
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

func (index *LumpIndex) RangeIter(start lump.LumpId, end lump.LumpId, fn func(lump.LumpId, portion.Portion) error) error {
	indexNum, value, ok := index.tree.First(start.U64())
	for ok && indexNum < end.U64() {
		portion, _ := fromValueToPortion(value)
		err := fn(lump.FromU64(0, indexNum), portion)
		if err != nil {
			return err
		}
		indexNum, value, ok = index.tree.Next(indexNum)
	}
	return nil
}

func (index *LumpIndex) ListRange(start lump.LumpId, end lump.LumpId, maxSize uint64) []lump.LumpId {
	vec := make([]lump.LumpId, 0, 1024)
	indexNum, _, ok := index.tree.First(start.U64())
	var n uint64 = 0
	for ok && indexNum < end.U64() && n < maxSize {
		vec = append(vec, lump.FromU64(0, indexNum))
		indexNum, _, ok = index.tree.Next(indexNum)
		n += 1
	}
	return vec
}

//return lumpID which is equal or greater then start
func (index *LumpIndex) First(start lump.LumpId) (lump.LumpId, error) {
	indexNum, _, ok := index.tree.First(start.U64())
	if !ok {
		return lump.EmptyLump(), errors.Wrapf(internalerror.InvalidInput, "failed to get first")
	}
	return lump.FromU64(0, indexNum), nil
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

//returns a JudyPorton
func fromDataPortionToJudy(p portion.DataPortion) uint64 {
	return (p.Start.AsU64() << 24) | uint64(p.Len)
}
