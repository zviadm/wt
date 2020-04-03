package wt

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSession(t *testing.T) {
	dbDir, err := ioutil.TempDir("", "wt_")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	c, err := Open(dbDir, ConnCfg{Create: True})
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
