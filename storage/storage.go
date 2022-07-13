package storage

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/syndtr/goleveldb/leveldb"
	ldbstorage "github.com/syndtr/goleveldb/leveldb/storage"
)

type columnSet struct {
	offset int
	fields []FieldInfo

	bitFields bitFieldStore
	kvStore   *kvStore
}

func Create(fields []FieldInfo) (*columnSet, error) {
	cs := &columnSet{}
	err := cs.init(fields)
	if err != nil {
		return nil, err
	}
	return cs, nil
}

func (cs *columnSet) init(fields []FieldInfo) error {
	for _, field := range fields {
		if field.Type == FieldTypeUnknown {
			return fmt.Errorf("unknown field type for field %q", field.Name)
		}
	}

	return nil
}

func (cs *columnSet) InsertRow(id int, row []string) error {

	return nil
}

type FieldType int

const (
	FieldTypeUnknown FieldType = iota
	FieldTypeBits
	FieldTypeKV
)

type FieldInfo struct {
	Name            string
	Type            FieldType
	CardinalityHint int
}

type bitFieldStore struct {
	fieldIndexes map[string]int
	fields       []*bitField
}

func (bfs *bitFieldStore) init(fields []FieldInfo) error {
	bfs.fieldIndexes = make(map[string]int, len(fields))
	bfs.fields = make([]*bitField, len(fields))

	for i, field := range fields {
		bfs.fieldIndexes[field.Name] = i
		bfs.fields[i] = &bitField{
			values:       make([]string, 0, field.CardinalityHint),
			valueIndexes: make(map[string]int, field.CardinalityHint),
			valueBits:    make([]*roaring.Bitmap, 0, field.CardinalityHint),
		}
	}

	return nil
}

func (bfs *bitFieldStore) insert(id int, field string, value string) error {
	if idx, ok := bfs.fieldIndexes[field]; ok {
		err := bfs.fields[idx].insert(id, value)
		if err != nil {
			return fmt.Errorf("insert into field %q: %w", field, err)
		}
	}

	return fmt.Errorf("unknown field: %q", field)
}

func (bfs *bitFieldStore) get(id int, field string) (string, error) {
	if idx, ok := bfs.fieldIndexes[field]; ok {
		v, err := bfs.fields[idx].get(id)
		if err != nil {
			return "", fmt.Errorf("get from field %q: %w", field, err)
		}
		return v, nil
	}

	return "", fmt.Errorf("unknown field: %q", field)
}

type bitField struct {
	values       []string
	valueIndexes map[string]int
	valueBits    []*roaring.Bitmap
}

func (bf *bitField) insert(id int, value string) error {
	var idx int
	var ok bool
	if idx, ok = bf.valueIndexes[value]; !ok {
		idx = len(bf.values)

		bf.values = append(bf.values, value)
		bf.valueBits = append(bf.valueBits, roaring.New())
		bf.valueIndexes[value] = idx
	}

	bf.valueBits[idx].AddInt(id)
	return nil
}

func (bf *bitField) get(id int) (string, error) {
	for idx, bs := range bf.valueBits {
		if bs.ContainsInt(id) {
			return bf.values[idx], nil
		}
	}

	return "", nil
}

type kvStore struct {
	db *leveldb.DB
}

func initKVStore() (*kvStore, error) {
	db, err := leveldb.Open(ldbstorage.NewMemStorage(), nil)
	if err != nil {
		return nil, err
	}

	return &kvStore{
		db: db,
	}, nil
}

func (k *kvStore) insert(id int, field string, value string) error {
	key := fmt.Sprintf("%d:%s", id, field)
	return k.db.Put([]byte(key), []byte(value), nil)
}

func (k *kvStore) get(id int, field string) (string, error) {
	key := fmt.Sprintf("%d:%s", id, field)
	res, err := k.db.Get([]byte(key), nil)
	return string(res), err
}
