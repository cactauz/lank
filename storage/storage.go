package storage

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/syndtr/goleveldb/leveldb"
	ldbstorage "github.com/syndtr/goleveldb/leveldb/storage"
)

type columnSet struct {
	fields []FieldInfo

	kvFields map[string]struct{}
	kvStore  *kvStore
	rbFields map[string]storedField
}

func CreateColumnSet(fields []FieldInfo) (*columnSet, error) {
	cs := &columnSet{
		fields:   fields,
		kvFields: make(map[string]struct{}, len(fields)),
		rbFields: make(map[string]storedField, len(fields)),
	}
	err := cs.init()
	if err != nil {
		return nil, err
	}
	return cs, nil
}

func (cs *columnSet) init() error {
	// TODO: the handling between field types is not ideal
	kvs := make([]FieldInfo, 0, len(cs.fields))

	for _, field := range cs.fields {
		switch field.Type {
		case FieldTypeUnknown:
			return fmt.Errorf("unknown field type for field %q", field.Name)
		case FieldTypeBitmapped:
			cs.rbFields[field.Name] = newBitField(field.CardinalityHint)
		case FieldTypeUintBits:
			// TODO: actually configure num bits or grow automatically
			cs.rbFields[field.Name] = newUintField(8)
		default:
			cs.kvFields[field.Name] = struct{}{}
			kvs = append(kvs, field)
		}
	}

	var err error
	cs.kvStore, err = initKVStore(kvs)
	if err != nil {
		return fmt.Errorf("init kv store: %w", err)
	}
	return nil
}

func (cs *columnSet) InsertRow(id int, row []any) error {
	for i, v := range row {
		field := cs.fields[i]
		if _, ok := cs.kvFields[field.Name]; ok {
			err := cs.kvStore.insert(id, field.Name, v)
			if err != nil {
				return err
			}
			continue
		}

		if f, ok := cs.rbFields[field.Name]; ok {
			err := f.insert(id, v)
			if err != nil {
				return err
			}
			continue
		}

		return fmt.Errorf("????")
	}

	return nil
}

func (cs *columnSet) GetRow(id int) ([]any, error) {
	row := make([]any, len(cs.fields))
	for i, field := range cs.fields {
		var err error
		if _, ok := cs.kvFields[field.Name]; ok {
			row[i], err = cs.kvStore.get(id, field.Name)
			if err != nil {
				return nil, err
			}
			continue
		}

		if f, ok := cs.rbFields[field.Name]; ok {
			row[i], err = f.get(id)
			if err != nil {
				return nil, err
			}
			continue
		}
	}

	return row, nil
}

type FieldType int

const (
	FieldTypeUnknown FieldType = iota
	FieldTypeBitmapped
	FieldTypeUintBits
	FieldTypeFloat
	FieldTypeString
)

type FieldInfo struct {
	Name            string
	Type            FieldType
	CardinalityHint int
}

type kvStore struct {
	db     *leveldb.DB
	fields map[string]kvField
}

func initKVStore(fields []FieldInfo) (*kvStore, error) {
	db, err := leveldb.Open(ldbstorage.NewMemStorage(), nil)
	if err != nil {
		return nil, err
	}

	fm := make(map[string]kvField)

	for _, field := range fields {
		switch field.Type {
		case FieldTypeFloat:
			fm[field.Name] = floatField{}
		case FieldTypeString:
			fm[field.Name] = stringField{}
		default:
			return nil, fmt.Errorf("unsupported type %q for kv store", field.Type)
		}
	}

	return &kvStore{
		db:     db,
		fields: fm,
	}, nil
}

func (k *kvStore) insert(id int, field string, value any) error {
	enc := k.fields[field].enc
	key := fmt.Sprintf("%d:%s", id, field)
	return k.db.Put([]byte(key), enc(value), nil)
}

func (k *kvStore) get(id int, field string) (any, error) {
	dec := k.fields[field].dec
	key := fmt.Sprintf("%d:%s", id, field)
	res, err := k.db.Get([]byte(key), nil)
	return dec(res), err
}

type kvField interface {
	enc(any) []byte
	dec([]byte) any
}

type floatField struct {
	kvField
}

func (floatField) enc(v any) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v.(float64)))
	return buf[:]
}

func (floatField) dec(v []byte) any {
	u := binary.LittleEndian.Uint64(v)
	return math.Float64frombits(u)
}

type stringField struct {
	kvField
}

func (stringField) enc(v any) []byte {
	return v.([]byte)
}

func (stringField) dec(v []byte) any {
	return string(v)
}
