package main

import (
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strconv"
	"time"

	"github.com/cactauz/lank/storage"
	"github.com/google/uuid"

	"net/http"
	_ "net/http/pprof"
)

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func printMemStats() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
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

			return strconv.Itoa(rng.Intn(nValues) + 1)
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

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	_ = make([]byte, 6*1024*1024*1024)

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
		panic(err)
	}

	rows := make([][]any, 10000)

	for i := 0; i < 10000; i++ {
		rows[i] = gen()
	}

	printMemStats()
	for i := 0; i < 3500000; i++ {
		err := cs.InsertRow(uint32(i), rows[rand.Intn(len(rows))])
		if err != nil {
			panic(err)
		}

		if i%10000 == 0 {
			fmt.Println(i)
			printMemStats()
		}
	}

	printMemStats()

	rows = nil
	_ = rows
	runtime.GC()
	fmt.Println("after gc")
	printMemStats()
	r, err := cs.GetRow(2473012)
	fmt.Println(r[:100], err)
}
