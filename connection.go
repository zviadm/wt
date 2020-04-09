package wt

/*
#include <stdlib.h>
#include <wiredtiger.h>

// Expose WT methods accessed through function pointers:
int wt_conn_close(
	WT_CONNECTION* connection,
	const char* config
	) {
    return connection->close(connection, config);
}
int wt_conn_open_session(
	WT_CONNECTION *connection,
	WT_EVENT_HANDLER *event_handler,
	const char *config,
	WT_SESSION **sessionp
	) {
    return connection->open_session(connection, event_handler, config, sessionp);
}
*/
import "C"

import (
	"unsafe"
)

type Connection struct {
	c *C.WT_CONNECTION
}

type Statistics string

const (
	StatsAll       Statistics = "all"
	StatsCacheWalk Statistics = "cache_walk"
	StatsClear     Statistics = "clear"
	StatsFast      Statistics = "fast"
	StatsNone      Statistics = "none"
	StatsTreeWalk  Statistics = "tree_walk"
)

type ConnCfg struct {
	CacheSize       int
	Create          wtBool
	Log             string
	SessionMax      int
	Statistics      []Statistics
	StatisticsLog   string
	TransactionSync string
}

func Open(path string, cfg ...ConnCfg) (*Connection, error) {
	pathC := C.CString(path)
	defer C.free(unsafe.Pointer(pathC))
	cfgC := configC(cfg)
	defer C.free(unsafe.Pointer(cfgC))

	c := &Connection{}
	if r := C.wiredtiger_open(pathC, nil, cfgC, &c.c); r != 0 {
		return nil, wtError(r)
	}
	return c, nil
}

type ConnCloseCfg struct {
	LeakMemory wtBool
}

func (c *Connection) Close(cfg ...ConnCloseCfg) error {
	cfgC := configC(cfg)
	defer C.free(unsafe.Pointer(cfgC))
	if r := C.wt_conn_close(c.c, cfgC); r != 0 {
		return wtError(r)
	}
	c.c = nil
	return nil
}

type SessionCfg struct {
	Isolation string
}

func (c *Connection) OpenSession(cfg ...SessionCfg) (*Session, error) {
	cfgC := configC(cfg)
	defer C.free(unsafe.Pointer(cfgC))

	s := &Session{}
	if r := C.wt_conn_open_session(c.c, nil, cfgC, &s.s); r != 0 {
		return nil, wtError(r)
	}
	return s, nil
}
