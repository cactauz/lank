package storage

import (
	"github.com/RoaringBitmap/roaring"
)

type fieldStore interface {
	init([]FieldInfo) error
	insert(fieldName string, id uint32, value any) error
	get(fieldName string, id uint32) (any, error)
}

type storedField interface {
	insert(id uint32, value any) error
	get(id uint32) (any, error)
}

var (
	_ storedField = &uintField{}
	_ storedField = &bitField{}
)

// type bitFieldStore struct {
// 	fieldIndexes map[string]int
// 	fields       []*bitField
// }
// func (bfs *bitFieldStore) init(fields []FieldInfo) error {
// 	bfs.fieldIndexes = make(map[string]int, len(fields))
// 	bfs.fields = make([]*bitField, len(fields))

// 	for i, field := range fields {
// 		bfs.fieldIndexes[field.Name] = i
// 		bfs.fields[i] = &bitField{
// 			values:       make([]any, 0, field.CardinalityHint),
// 			valueIndexes: make(map[any]int, field.CardinalityHint),
// 			valueBits:    make([]*roaring.Bitmap, 0, field.CardinalityHint),
// 		}
// 	}

// 	return nil
// }

// func (bfs *bitFieldStore) insert(id int, field string, value any) error {
// 	if idx, ok := bfs.fieldIndexes[field]; ok {
// 		err := bfs.fields[idx].insert(id, value)
// 		if err != nil {
// 			return fmt.Errorf("insert into field %q: %w", field, err)
// 		}
// 	}

// 	return fmt.Errorf("unknown field: %q", field)
// }

// func (bfs *bitFieldStore) get(id int, field string) (any, error) {
// 	if idx, ok := bfs.fieldIndexes[field]; ok {
// 		v, err := bfs.fields[idx].get(id)
// 		if err != nil {
// 			return "", fmt.Errorf("get from field %q: %w", field, err)
// 		}
// 		return v, nil
// 	}

// 	return "", fmt.Errorf("unknown field: %q", field)
// }

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

type uintField struct {
	setBits *roaring.Bitmap
	intBits []*roaring.Bitmap
}

func newUintField(nBits int) *uintField {
	uf := &uintField{
		setBits: roaring.New(),
		intBits: make([]*roaring.Bitmap, nBits),
	}

	for i := 0; i < nBits; i++ {
		uf.intBits[i] = roaring.New()
	}

	return uf
}

func (uf *uintField) insert(id uint32, value any) error {
	uf.setBits.Add(id)
	for i, rb := range uf.intBits {
		mask := 1 << i
		if value.(int)&mask == mask {
			rb.Add(id)
		}
	}

	return nil
}

func (uf *uintField) get(id uint32) (any, error) {
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
