package lumpindex

import (
	"github.com/google/btree"
	"github.com/pkg/errors"
	"github.com/thesues/cannyls-go/address"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/portion"
)

/*
	type assertion should be fast
	https://stackoverflow.com/questions/28024884/does-a-type-assertion-type-switch-have-bad-performance-is-slow-in-go
*/

type LumpIndex struct {
	tree *btree.BTree
}

type internalItem struct {
	id lump.LumpId
	n  uint64
}

func NewIndex() *LumpIndex {
	return &LumpIndex{
		tree: btree.New(32),
	}
}
func (index *LumpIndex) Get(id lump.LumpId) (p portion.Portion, err error) {
	key := internalItem{id: id, n: 0}
	item := index.tree.Get(key)
	if item == nil {
		return nil, errors.Wrapf(internalerror.InvalidInput, "failed to get key :%s", id.String())
	}

	p = fromItemToPortion(item)
	return p, nil
}

func (index *LumpIndex) InsertDataPortion(id lump.LumpId, data portion.DataPortion) {
	var n uint64 = 0
	n = data.Start.AsU64() | uint64(data.Len)<<40 | 1<<63
	key := internalItem{id: id, n: n}
	index.tree.ReplaceOrInsert(key)
}

func (index *LumpIndex) InsertJournalPortion(id lump.LumpId, data portion.JournalPortion) {
	var n uint64 = 0
	n = data.Start.AsU64() | uint64(data.Len)<<40
	key := internalItem{id: id, n: n}
	index.tree.ReplaceOrInsert(key)
}

func (index *LumpIndex) Delete(id lump.LumpId) portion.Portion {
	key := internalItem{id: id, n: 0}
	item := index.tree.Delete(key)

	return fromItemToPortion(item)
}

func fromItemToPortion(item btree.Item) portion.Portion {
	if item == nil {
		return nil
	}
	var p portion.Portion
	n := item.(internalItem).n
	//data portion
	kind := n >> 63
	len := uint16(n >> 40 & 0xFFFF)
	start := n & (address.MAX_ADDRESS)
	if kind == 0 {
		p = portion.NewJournalPortion(start, len)
	} else {
		p = portion.NewDataPortion(start, len)
	}
	return p
}

//bugy, the returned slice could be very large, should not be used production
func (index *LumpIndex) DeleteRange(start lump.LumpId, end lump.LumpId) {
	v := index.ListRange(start, end)
	for _, item := range v {
		index.Delete(item)
	}
}

//bugy, the returned slice could be very large, should not be used production
func (index *LumpIndex) List() []lump.LumpId {
	vec := make([]lump.LumpId, 0, 100)
	index.tree.Ascend(func(a btree.Item) bool {
		i := a.(internalItem)
		vec = append(vec, i.id)
		return true
	})
	return vec
}

//bugy, the returned slice could be very large, should not be used production
func (index *LumpIndex) ListRange(start lump.LumpId, end lump.LumpId) []lump.LumpId {
	vec := make([]lump.LumpId, 0, 100)
	startItem := internalItem{id: start, n: 0}
	endItem := internalItem{id: end, n: 0}
	index.tree.AscendRange(startItem, endItem, func(a btree.Item) bool {
		i := a.(internalItem)
		vec = append(vec, i.id)
		return true
	})
	return vec
}

func (index *LumpIndex) DataPortions() []*portion.DataPortion {
	vec := make([]*portion.DataPortion, 1)

	sentinel := portion.NewDataPortion(0, 0)
	vec[0] = &sentinel
	index.tree.Ascend(func(a btree.Item) bool {
		item := a.(internalItem)
		if (item.n >> 63) == 1 {
			len := uint16(item.n >> 40 & 0xFFFF)
			start := item.n & (address.MAX_ADDRESS)
			p := portion.NewDataPortion(start, len)
			vec = append(vec, &p)
		}
		return true
	})
	return vec
}

func (item internalItem) Less(than btree.Item) bool {
	left := item.id
	right := (than.(internalItem)).id
	if left.Compare(right) == -1 {
		return true
	} else {
		return false
	}
}
