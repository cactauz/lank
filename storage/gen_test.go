package storage

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"time"

	"github.com/google/uuid"
)

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("Alloc = %v", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

type genField struct {
	name string
	typ  FieldType
	gen  func() any
}

func genIdField(name string) genField {
	return genField{
		name: name,
		typ:  FieldTypeString,
		gen: func() any {
			return uuid.NewString()
		},
	}
}

func sparseField(nullPct float64, gen genField) genField {
	return genField{
		name: gen.name,
		typ:  gen.typ,
		gen: func() any {
			if nullPct != 0 && rng.Float64() < nullPct {
				return nil
			}

			return gen.gen()
		},
	}
}

func genFloatField(name string, min, max float64) genField {
	diff := max - min
	return genField{
		name: name,
		typ:  FieldTypeFloat,
		gen: func() any {
			return rng.Float64()*diff + min
		},
	}
}

func genBitmappedField(name string, nValues int) genField {
	return genField{
		name: name,
		typ:  FieldTypeBitmapped,
		gen: func() any {
			return strconv.Itoa(rng.Intn(nValues) + 1)
		},
	}
}

func genIntField(name string, maxValue int) genField {
	return genField{
		name: name,
		typ:  FieldTypeUintBits,
		gen: func() any {
			return rng.Intn(maxValue) % 256
		},
	}
}

func genBytesField(name string, minLength, maxLength int) genField {
	return genField{
		name: name,
		typ:  FieldTypeBytes,
		gen: func() any {
			bs := make([]byte, rand.Intn(maxLength-minLength)+minLength)
			rng.Read(bs)
			return bs
		},
	}
}

type row []any

func rowGenerator(fields []genField) func() row {
	return func() row {
		row := make([]any, len(fields))

		for i, f := range fields {
			row[i] = f.gen()
		}

		return row
	}
}

func genRows(n int, gen func() row) [][]any {
	rows := make([][]any, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, gen())
	}
	return rows
}
