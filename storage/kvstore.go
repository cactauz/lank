package storage

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/syndtr/goleveldb/leveldb"
	ldbstorage "github.com/syndtr/goleveldb/leveldb/storage"
)

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
