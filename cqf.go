package cqf

import ()

const (
	BitsPerSlot           = 8
	BlockOffsetBits       = 6
	SlotsPerBlock         = 1 << BlockOffsetBits
	MetadataWordsPerBlock = (SlotsPerBlock + 63) / 64
)

type block struct {
	offset    uint16
	occupieds [MetadataWordsPerBlock]uint64
	runends   [MetadataWordsPerBlock]uint64

	slots [SlotsPerBlock]uint8
}

type CQF struct {
	blocks []*block
}

func NewCQF() *CQF {
	return &CQF{}
}

func (c *CQF) Insert(item []byte, count uint64) {
}

func (c *CQF) InsertHash(hash uint64, count uint64) {
}

func (c *CQF) Count(item []byte) uint64 {
	return 0
}

func (c *CQF) CountHash(hash uint64) uint64 {
	return 0
}

func (c *CQF) Remove(item []byte, count uint64) {
}

func (c *CQF) RemoveHash(hash, count uint64) {
}

func (c *CQF) DeleteItem(item []byte) {
}
