package storage

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
)

// TODO: support negative ints
type intField struct {
	max     int
	setBits *roaring.Bitmap
	intBits []*roaring.Bitmap
}

func newIntField() *intField {
	uf := &intField{
		max:     0,
		setBits: roaring.New(),
		intBits: []*roaring.Bitmap{},
	}

	return uf
}

func (uf *intField) insert(id uint32, value any) error {
	v, ok := value.(int)
	if !ok {
		return fmt.Errorf("cannot insert %T into int field", value)
	}
	if uf.max-1 < v {
		uf.grow(v)
	}

	uf.setBits.Add(id)
	for i, rb := range uf.intBits {
		mask := 1 << i
		if v&mask == mask {
			rb.Add(id)
		}
	}

	return nil
}

// could be optimized with math/bits hacks :)
func (uf *intField) grow(min int) {
	for uf.max-1 < min {
		uf.intBits = append(uf.intBits, roaring.New())
		if uf.max == 0 {
			uf.max = 1
		}
		uf.max <<= 1
	}
}

func (uf *intField) get(id uint32) (any, error) {
	if !uf.setBits.Contains(id) {
		return nil, nil
	}

	var n int
	for i, rb := range uf.intBits {
		if rb.Contains(id) {
			n += 1 << i
		}
	}

	return n, nil
}
