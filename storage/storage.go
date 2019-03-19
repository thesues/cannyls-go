package storage

import (
	"github.com/thesues/cannyls-go/block"
	"github.com/thesues/cannyls-go/nvm"
)

var (
	MAGIC_NUMBER = [4]byte{'l', 'u', 's', 'f'}
)

const (
	MAJOR_VERSION           uint16 = 1
	MINOR_VERSION           uint16 = 1
	MAX_JOURNAL_REGION_SIZE uint64 = (1 << 40) - 1
	MAX_DATA_REGION_SIZE    uint64 = MAX_JOURNAL_REGION_SIZE * uint64(block.MIN)
)

type Storage struct {
	header nvm.StorageHeader
}

/*
pub struct Storage<N>
where
    N: NonVolatileMemory,
{
    header: StorageHeader,
    journal_region: JournalRegion<N>,
    data_region: DataRegion<N>,
    lump_index: LumpIndex,
    metrics: StorageMetrics,
}
*/
