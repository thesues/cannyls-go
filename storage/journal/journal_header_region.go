package journal

import (
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/nvm"
	"github.com/thesues/cannyls-go/util"
	"io"
)

func NewJournalHeadRegion(nvm nvm.NonVolatileMemory) *JournalHeaderRegion {
	return &JournalHeaderRegion{
		nvm: nvm,
	}
}

type JournalHeaderRegion struct {
	nvm nvm.NonVolatileMemory
}

func (headerRegion *JournalHeaderRegion) WriteTo(head uint64) (err error) {
	sector := headerRegion.nvm.BlockSize()
	ab := block.NewAlignedBytes(int(sector.AsU16()), sector)
	ab.Align()
	buf := ab.AsBytes()
	util.PutUINT64(buf, head)
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
	sector := headerRegion.nvm.BlockSize()
	ab := block.NewAlignedBytes(int(sector.AsU16()), sector)
	ab.Align()
	buf := ab.AsBytes()
	if _, err = headerRegion.nvm.Seek(0, io.SeekStart); err != nil {
		return
	}
	if _, err = headerRegion.nvm.Read(buf); err != nil {
		return
	}
	head = util.GetUINT64(buf[:8])
	return head, nil
}
