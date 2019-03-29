package journal

import (
	"bytes"
	"fmt"
	"testing"

	"encoding/hex"
	"github.com/stretchr/testify/assert"
	"github.com/thesues/cannyls-go/lump"
	"github.com/thesues/cannyls-go/portion"
)

func TestRecordWork(t *testing.T) {
	buf64KB := make([]byte, 0xFFFF)
	cases := []JournalRecord{
		EndOfRecords{},
		GoToFront{},
		PutRecord{
			LumpID:      lumpID("0A"),
			DataPortion: portion.NewDataPortion(0, 10),
		},
		PutRecord{
			LumpID:      lumpID("0A"),
			DataPortion: portion.NewDataPortion((1<<40)-1, 0xFFFF),
		},
		EmbedRecord{
			LumpID: lumpID("1111"),
			Data:   []byte("2222"),
		},
		EmbedRecord{
			LumpID: lumpID("1111"),
			Data:   buf64KB,
		},
		DeleteRecord{
			LumpID: lumpID("3333"),
		},
		DeleteRange{
			Start: lumpID("123A"),
			End:   lumpID("456B"),
		},
	}
	var _ = fmt.Printf
	var _ = hex.Dump
	var _ = portion.DefaultFreePortion
	buf := new(bytes.Buffer)

	for _, c := range cases {
		c.WriteTo(buf)
		c0, err := ReadRecordFrom(buf)
		assert.Nil(t, err)
		assert.Equal(t, c, c0)
	}
}

func TestRecordCheckSum(t *testing.T) {
	p := PutRecord{
		LumpID:      lumpID("0000"),
		DataPortion: portion.NewDataPortion(0, 10),
	}
	buf := new(bytes.Buffer)
	err := p.WriteTo(buf)
	assert.Nil(t, err)
	l := p.ExternalSize()
	readSlice := make([]byte, l)
	buf.Read(readSlice)
	readSlice[6] += 1
	_, err = ReadRecordFrom(bytes.NewBuffer(readSlice))
	assert.Error(t, err)
}

//helper funcion

func lumpID(s string) lump.LumpId {
	n, err := lump.FromString(s)
	if err != nil {
		panic("failed in create lump")
	}
	return n
}
