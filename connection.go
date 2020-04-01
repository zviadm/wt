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

type ConnectionConfig struct {
	Create          wtBool
	Log             string
	TransactionSync string
	SessionMax      int
}

func Open(path string, config *ConnectionConfig) (*Connection, error) {
	pathC := C.CString(path)
	defer C.free(unsafe.Pointer(pathC))
	cfgC := configC(config)
	defer C.free(unsafe.Pointer(cfgC))

	c := &Connection{}
	if r := C.wiredtiger_open(pathC, nil, cfgC, &c.c); r != 0 {
		return nil, wtError(r)
	}
	return c, nil
}

func (c *Connection) Close() error {
	if r := C.wt_conn_close(c.c, nil); r != 0 {
		return wtError(r)
	}
	c.c = nil
	return nil
}

type SessionConfig struct {
	Isolation string
}

func (c *Connection) OpenSession(config *SessionConfig) (*Session, error) {
	cfgC := configC(config)
	defer C.free(unsafe.Pointer(cfgC))

	s := &Session{}
	if r := C.wt_conn_open_session(c.c, nil, cfgC, &s.s); r != 0 {
		return nil, wtError(r)
	}
	return s, nil
}
