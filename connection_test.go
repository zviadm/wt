package wt

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpen(t *testing.T) {
	dbDir, err := ioutil.TempDir("", "wt_")
	require.NoError(t, err)
	defer os.RemoveAll(dbDir)

	_, err = Open(dbDir)
	require.Error(t, err)

	c, err := Open(dbDir, ConnCfg{
		Create:        True,
		Log:           "enabled,compressor=snappy",
		Statistics:    []Statistics{StatsAll, StatsClear},
		StatisticsLog: "wait=30",
	})
	require.NoError(t, err)
	err = c.Close()
	require.NoError(t, err)

	c, err = Open(dbDir)
	require.NoError(t, err)
	err = c.Close(ConnCloseCfg{LeakMemory: True}) // Leak memory, but this is ok, just in testing.
	require.NoError(t, err)
}
