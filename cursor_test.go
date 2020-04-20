package wt

import (
	"encoding/binary"
	"io/ioutil"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkCursorInsert-4 - 564543 - 2357 ns/op - 1.00 cgocalls/op - 0 B/op - 0 allocs/op
// This benchmark mainly exists to confirm that Insert call doesn't do any
// memory allocations.
func BenchmarkCursorInsert(b *testing.B) {
	s := setupDbForBench(b)
	c, err := s.OpenCursor("table:test_table")
	require.NoError(b, err)
	b.Cleanup(func() { c.Close() })

	insertK := []byte("testkeyXXXXXXXX")
	insertV := []byte("testvalXXXXXXXX")

	cgoCalls0 := runtime.NumCgoCall()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(insertK[len(insertK)-8:], uint64(i))
		binary.LittleEndian.PutUint64(insertV[len(insertV)-8:], uint64(i))
		err = c.Insert(insertK, insertV)
		if err != nil {
			b.Fatal(err) // Never use `require` library in benchmark hot path.
		}
	}
	b.ReportMetric(float64(runtime.NumCgoCall()-cgoCalls0)/float64(b.N), "cgocalls/op")
}

// BenchmarkCursorScan-4 - 1585243 - 722 ns/op - 3.00 cgocalls/op - 96 B/op - 2 allocs/op
func BenchmarkCursorScan(b *testing.B) {
	s := setupDbForBench(b)
	c, err := s.OpenCursor("table:test_table")
	require.NoError(b, err)
	b.Cleanup(func() { c.Close() })
	maxItems := b.N
	if maxItems > 10000 {
		maxItems = 10000
	}
	for i := 0; i < maxItems; i++ {
		err := c.Insert(
			[]byte("testkey"+strconv.Itoa(i)),
			[]byte("testval"+strconv.Itoa(i)))
		require.NoError(b, err)
	}

	err = c.Reset()
	require.NoError(b, err)
	cgoCalls0 := runtime.NumCgoCall()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err = c.Next()
		if err != nil {
			b.Fatal(err)
		}
		// UnsafeKey & UnsafeValue will each do one memory allocation to return the slice.
		// Even though no data is copied, GO slice structure still needs to be created and
		// allocated on heap.
		_, err = c.UnsafeKey()
		if err != nil {
			b.Fatal(err)
		}
		_, err = c.UnsafeValue()
		if err != nil {
			b.Fatal(err)
		}
		if (i+1)%maxItems == 0 {
			c.Reset() // Reset to start reading from the start again.
		}
	}
	b.ReportMetric(float64(runtime.NumCgoCall()-cgoCalls0)/float64(b.N), "cgocalls/op")
}

func setupDbForBench(b *testing.B) *Session {
	dbDir, err := ioutil.TempDir("", "wt_")
	require.NoError(b, err)
	b.Cleanup(func() { os.RemoveAll(dbDir) })

	c, err := Open(dbDir, ConnCfg{Create: True})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, c.Close()) })

	s, err := c.OpenSession()
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, s.Close()) })
	err = s.Create("table:test_table")
	require.NoError(b, err)
	return s
}
