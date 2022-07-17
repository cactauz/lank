package storage

import (
	"fmt"
	"testing"
)

func TestPrintBits(t *testing.T) {
	printBits(0, 8)
}

func FuzzBitField(f *testing.F) {

}

func printBits(v int, nBits int) {
	if v >= 1<<nBits {
		fmt.Println("2 big")
		return
	}

	res := make([]bool, nBits)
	mask := 1
	for i := 0; i < nBits; i++ {
		if v&mask == mask {
			res[i] = true
		} else {
			res[i] = false
		}
		mask <<= 1
	}

	fmt.Println(res)

	var n int
	for i, b := range res {
		if b {
			n += 1 << i
		}
	}

	fmt.Println(n)
}
