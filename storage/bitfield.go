package storage

import (
	"github.com/RoaringBitmap/roaring"
)

type bitField struct {
	values       []any
	valueIndexes map[any]int
	valueBits    []*roaring.Bitmap
}

func newBitField(cardinalityHint int) *bitField {
	return &bitField{
		values:       make([]any, 0, cardinalityHint),
		valueIndexes: make(map[any]int, cardinalityHint),
		valueBits:    make([]*roaring.Bitmap, 0, cardinalityHint),
	}
}

func (bf *bitField) insert(id uint32, value any) error {
	if value == nil {
		return nil
	}

	var idx int
	var ok bool
	if idx, ok = bf.valueIndexes[value]; !ok {
		idx = len(bf.values)
		bm := roaring.New()

		bf.values = append(bf.values, value)
		bf.valueBits = append(bf.valueBits, bm)
		bf.valueIndexes[value] = idx
	}

	bf.valueBits[idx].Add(id)
	return nil
}

func (bf *bitField) get(id uint32) (any, error) {
	for idx, bs := range bf.valueBits {
		if bs.Contains(id) {
			return bf.values[idx], nil
		}
	}

	return nil, nil
}
