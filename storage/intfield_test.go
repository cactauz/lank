package storage

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntField(t *testing.T) {
	t.Run("returns nil if not set", func(t *testing.T) {
		intf := newIntField()

		v, err := intf.get(0)
		assert.NoError(t, err)
		assert.Nil(t, v)

		v, err = intf.get(1298423)
		assert.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("returns error inserting non int", func(t *testing.T) {
		intf := newIntField()

		err := intf.insert(0, "hi :)")
		assert.EqualError(t, err, "cannot insert string into int field")
	})

	t.Run("doesnt store nils", func(t *testing.T) {
		assert := assert.New(t)
		bf := newBitField(0)

		err := bf.insert(1, nil)
		assert.NoError(err)

		v, err := bf.get(1)
		assert.NoError(err)
		assert.Nil(v)

		assert.Len(bf.values, 0)
		assert.Len(bf.valueIndexes, 0)
	})

	t.Run("grows to fit values", func(t *testing.T) {
		assert := assert.New(t)
		intf := newIntField()

		err := intf.insert(0, 212)
		assert.NoError(err)

		v, err := intf.get(0)
		assert.NoError(err)
		assert.Equal(212, v)

		assert.Equal(1<<8, intf.max)
		assert.Len(intf.intBits, 8)
	})

	t.Run("inserts and retrieves values", func(t *testing.T) {
		assert := assert.New(t)
		intf := newIntField()

		for _, s := range []struct {
			id uint32
			v  int
		}{
			{0, 0},
			{1, 1},
			{2, 1023},
			{3, 893247},
			{4, 17},
			{5, 1023},
			{6, math.MaxInt32},
		} {
			assert.NoError(intf.insert(s.id, s.v))

			v, err := intf.get(s.id)
			assert.NoError(err)
			assert.EqualValues(s.v, v)
		}
	})
}

func TestIntfieldSize(t *testing.T) {
	tests := []struct {
		v          int
		expectSize int
	}{
		{1, 1},
		{2, 2},
		{3, 2},
		{4, 3},
		{7, 3},
		{8, 4},
		{1023, 10},
		{1024, 11},
		{math.MaxInt32 / 2, 30},
		{math.MaxInt32, 31},
		{math.MaxInt64, 63},
	}

	for _, tt := range tests {
		t.Run(strconv.Itoa(tt.v), func(t *testing.T) {
			intf := newIntField()
			err := intf.insert(111, tt.v)
			assert.NoError(t, err)
			assert.Len(t, intf.intBits, tt.expectSize)
		})
	}
}
