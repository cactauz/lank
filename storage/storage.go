package storage

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/syndtr/goleveldb/leveldb"
	ldbstorage "github.com/syndtr/goleveldb/leveldb/storage"
)

type columnSet struct {
	fields  []FieldInfo
	sfields []storedField

	kvStore *kvStore
}

func CreateColumnSet(fields []FieldInfo) (*columnSet, error) {
	cs := &columnSet{
		fields:  fields,
		sfields: make([]storedField, 0, len(fields)),
		kvStore: &kvStore{},
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
			cs.sfields = append(cs.sfields, newBitField(field.CardinalityHint))
		case FieldTypeUintBits:
			// TODO: actually configure num bits or grow automatically
			cs.sfields = append(cs.sfields, newUintField(8))
		default:
			cs.sfields = append(cs.sfields, &kvWrapper{
				field: field.Name,
				store: cs.kvStore,
			})
			kvs = append(kvs, field)
		}
	}

	return cs.kvStore.init(kvs)
}

type kvWrapper struct {
	field string
	store *kvStore
}

func (kv *kvWrapper) insert(id uint32, value any) error {
	return kv.store.insert(id, kv.field, value)
}

func (kv *kvWrapper) get(id uint32) (any, error) {
	return kv.store.get(id, kv.field)
}

func (cs *columnSet) InsertRow(id uint32, row []any) error {
	for i, v := range row {
		if v == nil {
			continue
		}

		err := cs.sfields[i].insert(id, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cs *columnSet) GetRow(id uint32) ([]any, error) {
	row := make([]any, len(cs.fields))
	for i, field := range cs.sfields {
		var err error
		row[i], err = field.get(id)
		if err != nil {
			return nil, err
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

func (kv *kvStore) init(fields []FieldInfo) error {
	db, err := leveldb.Open(ldbstorage.NewMemStorage(), nil)
	if err != nil {
		return err
	}

	fm := make(map[string]kvField)

	for _, field := range fields {
		switch field.Type {
		case FieldTypeFloat:
			fm[field.Name] = floatField{}
		case FieldTypeString:
			fm[field.Name] = stringField{}
		default:
			return fmt.Errorf("unsupported type %q for kv store", field.Type)
		}
	}

	kv.db = db
	kv.fields = fm
	return nil
}

func (k *kvStore) insert(id uint32, field string, value any) error {
	enc := k.fields[field].enc
	key := fmt.Sprintf("%d:%s", id, field)
	return k.db.Put([]byte(key), enc(value), nil)
}

func (k *kvStore) get(id uint32, field string) (any, error) {
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
