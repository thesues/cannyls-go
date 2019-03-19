package lump

import (
	"github.com/google/btree"
	"github.com/thesues/cannyls-go/internalerror"
	"github.com/thesues/cannyls-go/portion"
)

type LumpIndex struct {
	tree *btree.BTree
}

type internalItem struct {
	id LumpId
	n  portion.FreePortion
}

func NewIndex() *LumpIndex {
	return &LumpIndex{
		tree: btree.New(2),
	}
}
func (index *LumpIndex) Get(id LumpId) (portion.FreePortion, error) {
	key := internalItem{id: id, n: portion.DefaultFreePortion()}
	n := index.tree.Get(key)
	if n == nil {
		return portion.DefaultFreePortion(), internalerror.InvalidInput
	}

	return (n.(internalItem)).n, nil

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
