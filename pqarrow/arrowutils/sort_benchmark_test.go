package arrowutils_test

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/polarsignals/frostdb/internal/records"
	"github.com/polarsignals/frostdb/pqarrow/arrowutils"
)

func BenchmarkSortRecord(b *testing.B) {
	b.StopTimer()
	build := records.NewBuild[Model](memory.NewGoAllocator())
	err := build.Append(makeModels(40)...)
	require.NoError(b, err)
	r := build.NewRecord()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := arrowutils.SortRecord(r.Record, r.SortingColumns)
		if err != nil {
			b.Fatal(err)
		}
	}
}

type Model struct {
	Timestamp  int64  `frostdb:",asc(0)"`
	Plain      string `frostdb:",asc(1)"`
	Dictionary string `frostdb:",asc(2)"`
}

func makeModels(n int) []Model {
	o := make([]Model, n)
	plain := makeRandomStrings(n)
	dict := makeRandomStrings(n)
	ts := makeRandomInts(n)

	// Simulate when we need to touch multiple columns

	// case 1
	// first column is equal second column is not
	span := 4
	limit := span + 2
	for i := span; i < n && i < limit; i++ {
		ts[i] = ts[span]
	}

	// case 2
	// both first and second column are equal
	span = limit + 1
	limit = span + 4
	for i := span; i < n && i < limit; i++ {
		ts[i] = ts[span]
		plain[i] = plain[span]
	}
	// case 3
	// all three columns are equal
	span = limit + 1
	limit = span + 4
	for i := span; i < n && i < limit; i++ {
		ts[i] = ts[span]
		plain[i] = plain[span]
		dict[i] = dict[span]
	}
	for i := range o {
		o[i] = Model{
			Timestamp:  ts[i],
			Plain:      plain[i],
			Dictionary: dict[i],
		}
	}
	return o
}

func makeRandomInts(n int) []int64 {
	r := rand.New(rand.NewSource(42))
	ints := make([]int64, n)
	for i := 0; i < n; i++ {
		ints[i] = r.Int63n(int64(n))
	}
	return ints
}

func makeRandomStrings(n int) []string {
	r := rand.New(rand.NewSource(42))
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	ss := make([]string, n)
	for i := 0; i < n; i++ {
		var sb strings.Builder
		slen := 2 + rand.Intn(50)
		for j := 0; j < slen; j++ {
			sb.WriteRune(letters[r.Intn(len(letters))])
		}
		ss[i] = sb.String()
	}
	return ss
}
