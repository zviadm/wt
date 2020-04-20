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

	c, err := Open(dbDir, ConnCfg{Create: True, Log: "enabled"})
	require.NoError(t, err)
	defer func() { require.NoError(t, c.Close()) }()

	s, err := c.OpenSession()
	require.NoError(t, err)
	defer func() { require.NoError(t, s.Close()) }()

	err = s.Create("table:test_table", DataSourceCfg{BlockCompressor: "snappy"})
	require.NoError(t, err)

	cc, err := s.OpenCursor("table:test_table")
	require.NoError(t, err)
	err = cc.Insert([]byte("testkey1"), []byte("testvalue1"))
	require.NoError(t, err)
	err = cc.Insert([]byte("testkey2"), []byte("testvalue2"))
	require.NoError(t, err)

	require.NoError(t, err)
	v, err := cc.ReadUnsafeValue([]byte("testkey1"))
	require.NoError(t, err)
	require.EqualValues(t, []byte("testvalue1"), v)

	err = cc.Search([]byte("testkey2"))
	require.NoError(t, err)
	v, err = cc.UnsafeValue()
	require.NoError(t, err)
	require.EqualValues(t, []byte("testvalue2"), v)

	err = cc.Search([]byte("testkey3"))
	require.Error(t, err)
	require.EqualValues(t, ErrNotFound, ErrCode(err))

	near, err := cc.SearchNear([]byte("testkey3"))
	require.NoError(t, err)
	require.EqualValues(t, MatchedSmaller, near)
	v, err = cc.UnsafeValue()
	require.NoError(t, err)
	require.EqualValues(t, []byte("testvalue2"), v)

	err = cc.Insert([]byte("testempty1"), []byte{})
	require.NoError(t, err)
	err = cc.Insert([]byte("testempty2"), nil)
	require.NoError(t, err)
	v, err = cc.ReadUnsafeValue([]byte("testempty1"))
	require.NoError(t, err)
	require.EqualValues(t, []byte(nil), v)
	v, err = cc.ReadUnsafeValue([]byte("testempty2"))
	require.NoError(t, err)
	require.EqualValues(t, []byte(nil), v)

	err = s.LogFlush(SyncOn) // Flush log for good measure.
	require.NoError(t, err)

	err = s.Drop("table:test_table")
	require.Error(t, err) // can't drop until cursors are closed.
	require.NoError(t, cc.Close())

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
	c1, err := s1.OpenCursor("table:test_table")
	require.NoError(t, err)
	defer c1.Close()
	err = c1.Insert([]byte("testkey1"), []byte("testvalue1"))
	require.NoError(t, err)

	// value written in transaction must not be visible.
	c2, err := s2.OpenCursor("table:test_table")
	require.NoError(t, err)
	_, err = c2.ReadUnsafeValue([]byte("testkey1"))
	require.Error(t, err)
	require.EqualValues(t, ErrNotFound, ErrCode(err))

	err = s1.TxCommit()
	require.NoError(t, err)

	// once transaction commits, value should be visible.
	v, err := c2.ReadUnsafeValue([]byte("testkey1"))
	require.NoError(t, err)
	require.EqualValues(t, []byte("testvalue1"), v)
}
