package storage

import (
	"fmt"
)

type FieldType int

const (
	FieldTypeUnknown FieldType = iota
	FieldTypeBitmapped
	FieldTypeUintBits
	FieldTypeFloat
	FieldTypeBytes
	FieldTypeString
	FieldTypeTimestamp
)

type FieldInfo struct {
	Name            string
	Type            FieldType
	CardinalityHint int
}

type rowset struct {
	fields  []FieldInfo
	sfields []storedField

	kvStore *kvStore
}

func CreateRowset(fields []FieldInfo) (*rowset, error) {
	rs := &rowset{
		fields:  fields,
		sfields: make([]storedField, 0, len(fields)),
		kvStore: &kvStore{},
	}
	err := rs.init()
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (rs *rowset) init() error {
	// TODO: the handling between field types is not ideal
	kvs := make([]FieldInfo, 0, len(rs.fields))

	for _, field := range rs.fields {
		switch field.Type {
		case FieldTypeUnknown:
			return fmt.Errorf("unknown field type for field %q", field.Name)
		case FieldTypeBitmapped:
			rs.sfields = append(rs.sfields, newBitField(field.CardinalityHint))
		case FieldTypeUintBits:
			rs.sfields = append(rs.sfields, newIntField())
		default:
			rs.sfields = append(rs.sfields, &kvWrapper{
				field: field.Name,
				store: rs.kvStore,
			})
			kvs = append(kvs, field)
		}
	}

	return rs.kvStore.init(kvs)
}

func (rs *rowset) InsertRow(id uint32, row []any) error {
	for i, v := range row {
		if v == nil {
			continue
		}

		err := rs.sfields[i].insert(id, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (rs *rowset) GetRow(id uint32) ([]any, error) {
	row := make([]any, len(rs.fields))
	for i, field := range rs.sfields {
		var err error
		row[i], err = field.get(id)
		if err != nil {
			return nil, err
		}
	}

	return row, nil
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

type storedField interface {
	insert(id uint32, value any) error
	get(id uint32) (any, error)
}
