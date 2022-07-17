package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/cactauz/lank/storage"
	"github.com/google/uuid"
)

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v", m.Alloc)
	fmt.Printf("\tTotalAlloc = %v", m.TotalAlloc)
	fmt.Printf("\tSys = %v", m.Sys)
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func TestMain(t *testing.T) {
	r1 := roaring.New()
	r2 := roaring.New()

	printMemStats()

	for i := 0; i < 30000000; i++ {
		n := rng.Intn(6)
		if n == 0 {
			r1.AddInt(i)
		}
		if i%3 == 0 {
			r2.AddInt(i)
		}
	}
	runtime.GC()
	printMemStats()
	fmt.Println("c1:", r1.GetCardinality())
	fmt.Println("c2:", r2.GetCardinality())
	fmt.Println("optimize =>")
	r1.RunOptimize()
	r2.RunOptimize()
	fmt.Println("<= optimize")
	runtime.GC()
	printMemStats()

	start := time.Now()
	a1 := r1.Clone()
	a1.And(r2)
	fmt.Println(time.Since(start))

	fmt.Println(a1.GetCardinality())
	printMemStats()
	// n := 0
	// iter := r1.ManyIterator()

	// for {
	// 	c := iter.NextMany(buf)
	// 	if c == 0 {
	// 		break
	// 	}

	// 	n += c
	// }
	// fmt.Println("total counted:", n)

}

var floats = map[uint32]float64{
	0: 2.344358,
	1: .772838,
	2: 1.28370,
	3: .876447,
	4: .9884740,
}

func TestSumFloats(t *testing.T) {
	sum := 0.0
	start := time.Now()
	for i := 0; i < 10000000; i++ {
		n := i % 5

		sum += floats[uint32(n)]
	}
	fmt.Println(time.Since(start), sum)
}

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

type genField struct {
	name string
	typ  storage.FieldType
	gen  func() any
}

func idField(name string) genField {
	return genField{
		name: name,
		typ:  storage.FieldTypeString,
		gen: func() any {
			bs, _ := uuid.New().MarshalBinary()
			return bs
		},
	}
}

func floatField(name string, min, max float64, nullPct float64) genField {
	diff := max - min
	return genField{
		name: name,
		typ:  storage.FieldTypeFloat,
		gen: func() any {
			if nullPct != 0 && rng.Float64() < nullPct {
				return nil
			}

			return rng.Float64()*diff + min
		},
	}
}

func bitmappedField(name string, nValues int, nullPct float64) genField {
	return genField{
		name: name,
		typ:  storage.FieldTypeBitmapped,
		gen: func() any {
			if nullPct != 0 && rng.Float64() < nullPct {
				return nil
			}

			return "value_" + strconv.Itoa(rng.Intn(nValues)+1)
		},
	}
}

func uint8Field(name string, maxValue int, nullPct float64) genField {
	return genField{
		name: name,
		typ:  storage.FieldTypeUintBits,
		gen: func() any {
			if nullPct != 0 && rng.Float64() < nullPct {
				return nil
			}

			return rng.Intn(maxValue) % 256
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

func TestInsertData(t *testing.T) {
	fields := []genField{
		idField("id"),
		floatField("wts", .75, 1.25, 0),
		uint8Field("age", 99, 0),
		bitmappedField("gdr", 5, 0),
		bitmappedField("region", 10, 0),
		bitmappedField("lang", 4, 0),
	}

	for i := 0; i < 15000; i++ {
		field := bitmappedField(fmt.Sprintf("question_%d", i), 6, .95)
		fields = append(fields, field)
	}

	gen := rowGenerator(fields)

	fieldInfos := make([]storage.FieldInfo, 0, len(fields))

	for _, f := range fields {
		fieldInfos = append(fieldInfos, storage.FieldInfo{
			Name:            f.name,
			Type:            f.typ,
			CardinalityHint: 6,
		})
	}

	cs, err := storage.CreateColumnSet(fieldInfos)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000000; i++ {
		row := gen()
		err := cs.InsertRow(i, row)
		if err != nil {
			t.Fatal(err)
		}

		if i%10000 == 0 {
			fmt.Println(i)
		}
	}
}
