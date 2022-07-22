package storage

import (
	"fmt"
	"testing"
)

func TestInsertData(t *testing.T) {
	fields := []genField{
		genIdField("id"),
		genFloatField("score", .75, 1.25, 0),
		genIntField("age", 99, 0),
		genBitmappedField("zone", 5, 0),
		genBitmappedField("continent", 10, 0),
		genBitmappedField("quadrant", 4, 0),
	}

	for i := 0; i < 15000; i++ {
		field := genBitmappedField(fmt.Sprintf("question_%d", i), 6, .95)
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
