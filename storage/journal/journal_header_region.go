package journal

import (
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/util"
	"io"
)

func NewJournalHeadRegion(nvm nvm.NonVolatileMemory) *JournalHeaderRegion {
	ab := block.NewAlignedBytes(int(block.MIN), block.Min())
	ab.Align()
	return &JournalHeaderRegion{
		nvm: nvm,
		ab:  ab,
	}
}

type JournalHeaderRegion struct {
	nvm nvm.NonVolatileMemory
	ab  *block.AlignedBytes
}

func (headerRegion *JournalHeaderRegion) WriteTo(head uint64) (err error) {
	buf := headerRegion.ab.AsBytes()
	util.PutUINT64(buf[:8], head)
	if _, err = headerRegion.nvm.Seek(0, io.SeekStart); err != nil {
		return
	}
	if _, err = headerRegion.nvm.Write(buf); err != nil {
		return
	}

	return headerRegion.nvm.Sync()
}

func (headerRegion *JournalHeaderRegion) ReadFrom() (head uint64, err error) {
	head = 0
	buf := headerRegion.ab.AsBytes()
	if _, err = headerRegion.nvm.Seek(0, io.SeekStart); err != nil {
		return
	}
	if _, err = headerRegion.nvm.Read(buf); err != nil {
		return
	}
	head = util.GetUINT64(buf[:8])
	return head, nil
}
