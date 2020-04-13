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

func TestSession(t *testing.T) {
	dbDir, err := ioutil.TempDir("", "wt_")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	c, err := Open(dbDir, ConnCfg{Create: True, Log: "enabled"})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	s, err := c.OpenSession()
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()

	err = s.Create("table:test_table", DataSourceCfg{BlockCompressor: "snappy"})
	require.NoError(t, err)

	m, err := s.Mutate("table:test_table")
	require.NoError(t, err)
	err = m.Insert([]byte("testkey1"), []byte("testvalue1"))
	require.NoError(t, err)
	err = m.Insert([]byte("testkey2"), []byte("testvalue2"))
	require.NoError(t, err)

	scan, err := s.Scan("table:test_table")
	require.NoError(t, err)
	v, err := scan.ReadUnsafeValue([]byte("testkey1"))
	require.NoError(t, err)
	require.EqualValues(t, []byte("testvalue1"), v)

	err = scan.Search([]byte("testkey2"))
	require.NoError(t, err)
	v, err = scan.UnsafeValue()
	require.NoError(t, err)
	require.EqualValues(t, []byte("testvalue2"), v)

	err = scan.Search([]byte("testkey3"))
	require.Error(t, err)
	require.EqualValues(t, ErrNotFound, ErrCode(err))

	near, err := scan.SearchNear([]byte("testkey3"))
	require.NoError(t, err)
	require.EqualValues(t, MatchedSmaller, near)
	v, err = scan.UnsafeValue()
	require.NoError(t, err)
	require.EqualValues(t, []byte("testvalue2"), v)

	err = m.Insert([]byte("testempty1"), []byte{})
	require.NoError(t, err)
	err = m.Insert([]byte("testempty2"), nil)
	require.NoError(t, err)
	v, err = scan.ReadUnsafeValue([]byte("testempty1"))
	require.NoError(t, err)
	require.EqualValues(t, []byte(nil), v)
	v, err = scan.ReadUnsafeValue([]byte("testempty2"))
	require.NoError(t, err)
	require.EqualValues(t, []byte(nil), v)

	err = s.LogFlush(SyncOn) // Flush log for good measure.
	require.NoError(t, err)

	err = s.Drop("table:test_table")
	require.Error(t, err) // can't drop until cursors are closed.
	require.NoError(t, m.Close())
	require.NoError(t, scan.Close())

	err = s.Drop("table:test_table")
	require.NoError(t, err)
}

func TestSessionTxs(t *testing.T) {
	dbDir, err := ioutil.TempDir("", "wt_")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	c, err := Open(dbDir, ConnCfg{Create: True})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	s1, err := c.OpenSession()
	require.NoError(t, err)
	defer func() { require.NoError(t, s1.Close()) }()
	err = s1.Create("table:test_table")
	require.NoError(t, err)

	s2, err := c.OpenSession()
	require.NoError(t, err)
	defer func() { require.NoError(t, s2.Close()) }()

	err = s1.TxBegin()
	require.NoError(t, err)
	m, err := s1.Mutate("table:test_table")
	require.NoError(t, err)
	defer m.Close()
	err = m.Insert([]byte("testkey1"), []byte("testvalue1"))
	require.NoError(t, err)

	// value written in transaction must not be visible.
	scan, err := s2.Scan("table:test_table")
	require.NoError(t, err)
	_, err = scan.ReadUnsafeValue([]byte("testkey1"))
	require.Error(t, err)
	require.EqualValues(t, ErrNotFound, ErrCode(err))

	err = s1.TxCommit()
	require.NoError(t, err)

	// once transaction commits, value should be visible.
	v, err := scan.ReadUnsafeValue([]byte("testkey1"))
	require.NoError(t, err)
	require.EqualValues(t, []byte("testvalue1"), v)
}

// BenchmarkCursorInsert-4 - 272791 - 4897 ns/op - 1.00 cgocalls/op - 0 B/op - 0 allocs/op
// This benchmark mainly exists to confirm that Insert call doesn't do any
// memory allocations.
func BenchmarkCursorInsert(b *testing.B) {
	dbDir, err := ioutil.TempDir("", "wt_")
	require.NoError(b, err)
	defer os.RemoveAll(dbDir)

	c, err := Open(dbDir, ConnCfg{Create: True})
	require.NoError(b, err)
	defer func() { require.NoError(b, c.Close()) }()

	s, err := c.OpenSession()
	require.NoError(b, err)
	defer func() { require.NoError(b, s.Close()) }()
	err = s.Create("table:test_table")
	require.NoError(b, err)

	m, err := s.Mutate("table:test_table")
	require.NoError(b, err)
	defer m.Close()

	insertK := []byte("testkeyXXXXXXXX")
	insertV := []byte("testvalXXXXXXXX")

	cgoCalls0 := runtime.NumCgoCall()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		binary.LittleEndian.PutUint64(insertK[len(insertK)-8:], uint64(i))
		binary.LittleEndian.PutUint64(insertV[len(insertV)-8:], uint64(i))
		err = m.Insert(insertK, insertV)
		if err != nil {
			b.Fatal(err) // Never use `require` library in benchmark hot path.
		}
	}
	b.ReportMetric(float64(runtime.NumCgoCall()-cgoCalls0)/float64(b.N), "cgocalls/op")
}

// BenchmarkCursorScan-4 - 1384462 - 781 ns/op - 3.00 cgocalls/op - 96 B/op - 2 allocs/op
func BenchmarkCursorScan(b *testing.B) {
	dbDir, err := ioutil.TempDir("", "wt_")
	require.NoError(b, err)
	defer os.RemoveAll(dbDir)

	c, err := Open(dbDir, ConnCfg{Create: True})
	require.NoError(b, err)
	defer func() { require.NoError(b, c.Close()) }()

	s, err := c.OpenSession()
	require.NoError(b, err)
	defer func() { require.NoError(b, s.Close()) }()
	err = s.Create("table:test_table")
	require.NoError(b, err)

	m, err := s.Mutate("table:test_table")
	require.NoError(b, err)
	maxItems := b.N
	if maxItems > 10000 {
		maxItems = 10000
	}
	for i := 0; i < maxItems; i++ {
		err := m.Insert(
			[]byte("testkey"+strconv.Itoa(i)),
			[]byte("testval"+strconv.Itoa(i)))
		require.NoError(b, err)
	}
	m.Close()

	scan, err := s.Scan("table:test_table")
	require.NoError(b, err)

	cgoCalls0 := runtime.NumCgoCall()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err = scan.Next()
		if err != nil {
			b.Fatal(err)
		}
		// UnsafeKey & UnsafeValue will each do one memory allocation to return the slice.
		// Even though no data is copied, GO slice structure still needs to be created and
		// allocated on heap.
		_, err = scan.UnsafeKey()
		if err != nil {
			b.Fatal(err)
		}
		_, err = scan.UnsafeValue()
		if err != nil {
			b.Fatal(err)
		}
		if (i+1)%maxItems == 0 {
			scan.Reset() // Reset to start reading from the start again.
		}
	}
	b.ReportMetric(float64(runtime.NumCgoCall()-cgoCalls0)/float64(b.N), "cgocalls/op")
}
