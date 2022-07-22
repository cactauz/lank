package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitField(t *testing.T) {
	t.Run("returns nil if not set", func(t *testing.T) {
		bf := newBitField(0)

		v, err := bf.get(0)
		assert.NoError(t, err)
		assert.Nil(t, v)
	})

	t.Run("doesnt store nils", func(t *testing.T) {
		bf := newBitField(0)

		assert := assert.New(t)
		err := bf.insert(1, nil)
		assert.NoError(err)

		v, err := bf.get(1)
		assert.NoError(err)
		assert.Nil(v)

		assert.Len(bf.values, 0)
		assert.Len(bf.valueIndexes, 0)
	})

	t.Run("inserts and retrieves values", func(t *testing.T) {
		assert := assert.New(t)
		bf := newBitField(5)

		for _, s := range []struct {
			id uint32
			v  any
		}{
			{0, "a"},
			{1, "b"},
			{2, "c"},
			{3, "d"},
			{4, 4},
			{5, "a"},
			{6, complex64(2 + 1i)},
		} {
			assert.NoError(bf.insert(s.id, s.v))

			v, err := bf.get(s.id)
			assert.NoError(err)
			assert.EqualValues(s.v, v)
		}

		assert.Len(bf.values, 6)
		assert.Len(bf.valueIndexes, 6)
	})
}
