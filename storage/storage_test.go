package storage

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRowInsertGet(t *testing.T) {
	assert := assert.New(t)
	fields := []genField{
		genIdField("id"),
		genFloatField("float", .75, 1.25),
		genIntField("int", 99),
		genBitmappedField("bitmap", 4),
		genBytesField("bytes", 10, 250),
	}

	fieldInfos := make([]FieldInfo, 0, len(fields))

	for _, f := range fields {
		fieldInfos = append(fieldInfos, FieldInfo{
			Name:            f.name,
			Type:            f.typ,
			CardinalityHint: 6,
		})
	}

	rowset, err := CreateRowset(fieldInfos)
	assert.NoError(err)

	gen := rowGenerator(fields)
	rows := genRows(100, gen)
	for i, row := range rows {
		rowset.InsertRow(uint32(i), row)
	}

	for i := 0; i < 100; i++ {
		got, err := rowset.GetRow(uint32(i))
		assert.NoError(err)
		assert.EqualValues(rows[i], got)
	}

}

func TestInsertMemStats(t *testing.T) {
	fields := []genField{
		genIdField("id"),
		genFloatField("score", .75, 1.25),
		genIntField("age", 99),
		genBitmappedField("zone", 5),
		genBitmappedField("continent", 10),
		genBitmappedField("quadrant", 4),
	}

	for i := 0; i < 15000; i++ {
		field := sparseField(0.95,
			genBitmappedField(fmt.Sprintf("question_%d", i), 6),
		)
		fields = append(fields, field)
	}

	gen := rowGenerator(fields)

	fieldInfos := make([]FieldInfo, 0, len(fields))

	for _, f := range fields {
		fieldInfos = append(fieldInfos, FieldInfo{
			Name:            f.name,
			Type:            f.typ,
			CardinalityHint: 6,
		})
	}

	rs, err := CreateRowset(fieldInfos)
	if err != nil {
		t.Fatal(err)
	}

	// these rows are slow to gen and take a ton of space.
	// generate some in advance and choose from them randomly.
	rows := make([][]any, 10000)
	for i := 0; i < 10000; i++ {
		rows[i] = gen()
	}

	t.Run("insert", func(t *testing.T) {
		printMemStats()
		for i := 0; i < 100000; i++ {
			err := rs.InsertRow(uint32(i), rows[rng.Intn(len(rows))])
			if err != nil {
				t.Fatal(err)
			}

			if i%10000 == 0 {
				fmt.Println(i)
			}
		}
		printMemStats()
	})
}
