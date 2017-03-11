package quickFind

import (
	"fmt"
	"testing"
)

var (
	qftestSuite = []struct{ from, to int }{
		{4, 3},
		{3, 8},
		{6, 5},
		{9, 4},
		{2, 1},
		{5, 0},
		{7, 2},
		{6, 1},
	}
)

//TODO: Proper testing
func TestUnion(t *testing.T) {
	qfTestT := InitializeQF(10)
	for _, pair := range qftestSuite {
		fmt.Println(qfTestT.Connected(pair.from, pair.to))
		qfTestT.Union(pair.from, pair.to)
		fmt.Println(qfTestT.Connected(pair.from, pair.to))
		fmt.Printf("Total components:%v\n", qfTestT.GetComponents())
	}

}
