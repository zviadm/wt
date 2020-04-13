package wt

/*
#include <stdlib.h>
#include <wiredtiger.h>

// Expose WT methods accessed through function pointers:
int wt_conn_close(
	WT_CONNECTION* connection,
	_GoString_ config
	) {
    return connection->close(connection, _GoStringPtr(config));
}
int wt_conn_open_session(
	WT_CONNECTION *connection,
	WT_EVENT_HANDLER *event_handler,
	_GoString_ config,
	WT_SESSION **sessionp
	) {
    return connection->open_session(connection, event_handler, _GoStringPtr(config), sessionp);
}
*/
import "C"

import (
	"unsafe"
)

// Connection is a wrapper for WT_CONNECTION class.
type Connection struct {
	c *C.WT_CONNECTION
}

// Statistics configuration options enum.
type Statistics string

// Statistics configuration options.
const (
	StatsAll       Statistics = "all"
	StatsCacheWalk Statistics = "cache_walk"
	StatsClear     Statistics = "clear"
	StatsFast      Statistics = "fast"
	StatsNone      Statistics = "none"
	StatsTreeWalk  Statistics = "tree_walk"
)

// ConnCfg mirrors options for wiredtiger_open call.
type ConnCfg struct {
	CacheSize       int
	Create          wtBool
	Log             string
	SessionMax      int
	Statistics      []Statistics
	StatisticsLog   string
	TransactionSync string
}

// Open performs wiredtiger_open call.
func Open(path string, cfg ...ConnCfg) (*Connection, error) {
	pathC := C.CString(path)
	defer C.free(unsafe.Pointer(pathC))
	cfgC := C.CString(configC(cfg))
	defer C.free(unsafe.Pointer(cfgC))
	c := &Connection{}
	if r := C.wiredtiger_open(pathC, nil, cfgC, &c.c); r != 0 {
		return nil, wtError(r)
	}
	return c, nil
}

// ConnCloseCfg mirrors options for WT_CONNECTION::close call.
type ConnCloseCfg struct {
	LeakMemory wtBool
}

// Close performs WT_CONNECTION::close call.
func (c *Connection) Close(cfg ...ConnCloseCfg) error {
	cfgC := configC(cfg)
	if r := C.wt_conn_close(c.c, cfgC); r != 0 {
		return wtError(r)
	}
	c.c = nil
	return nil
}

// SessionCfg mirrors options for WT_CONNECTION::open_session call.
type SessionCfg struct {
	Isolation string
}

// OpenSession performs WT_CONNECTION::open_session call.
func (c *Connection) OpenSession(cfg ...SessionCfg) (*Session, error) {
	cfgC := configC(cfg)
	s := &Session{}
	if r := C.wt_conn_open_session(c.c, nil, cfgC, &s.s); r != 0 {
		return nil, wtError(r)
	}
	return s, nil
}
