package wt

/*
#include <stdlib.h>
#include <wiredtiger.h>

// Expose WT methods accessed through function pointers:
int wt_session_close(
	WT_SESSION *session,
	const char *config
	) {
    return session->close(session, config);
}
int wt_session_create(
	WT_SESSION *session,
	const char *name,
	const char *config
	) {
    return session->create(session, name, config);
}
int wt_session_drop(
	WT_SESSION *session,
	const char *name,
	const char *config
	) {
    return session->drop(session, name, config);
}
int wt_session_open_cursor(
	WT_SESSION *session,
	const char *uri,
	WT_CURSOR *to_dup,
	const char *config,
	WT_CURSOR **cursorp
	) {
    return session->open_cursor(session, uri, to_dup, config, cursorp);
}
int wt_session_log_flush(
	WT_SESSION *session,
	const char *config
	) {
    return session->log_flush(session, config);
}
int wt_session_begin_transaction(
	WT_SESSION *session,
	const char *config
	) {
    return session->begin_transaction(session, config);
}
int wt_session_commit_transaction(
	WT_SESSION *session,
	const char *config
	) {
    return session->commit_transaction(session, config);
}
int wt_session_rollback_transaction(
	WT_SESSION *session,
	const char *config
	) {
    return session->rollback_transaction(session, config);
}
*/
import "C"

import (
	"unsafe"
)

type Session struct {
	s    *C.WT_SESSION
	inTx bool
}

func (s *Session) Close() error {
	r := C.wt_session_close(s.s, nil)
	s.s = nil
	if r != 0 {
		return wtError(r)
	}
	return nil
}

func (s *Session) Closed() bool {
	return s.s == nil
}

type DataSourceCfg struct {
	BlockCompressor string
}

func (s *Session) Create(name string, cfg ...DataSourceCfg) error {
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	cfgC := configC(cfg)
	defer C.free(unsafe.Pointer(cfgC))

	if r := C.wt_session_create(s.s, nameC, cfgC); r != 0 {
		return wtError(r)
	}
	return nil
}

type DropCfg struct {
	Force       wtBool
	RemoveFiles wtBool
}

func (s *Session) Drop(name string, cfg ...DropCfg) error {
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	cfgC := configC(cfg)
	defer C.free(unsafe.Pointer(cfgC))

	r := C.wt_session_drop(s.s, nameC, cfgC)
	return wtError(r)
}

type MutateCfg struct {
	Overwrite wtBool
	Bulk      wtBool
	raw       wtBool
}

func (s *Session) Mutate(uri string, cfg ...MutateCfg) (*Mutator, error) {
	uriC := C.CString(uri)
	defer C.free(unsafe.Pointer(uriC))
	var cfgC *C.char
	if len(cfg) >= 1 {
		cfg[0].raw = True
		cfgC = configC(cfg)
	} else {
		cfgC = C.CString("raw")
	}
	defer C.free(unsafe.Pointer(cfgC))
	c := &Mutator{}
	r := C.wt_session_open_cursor(s.s, uriC, nil, cfgC, &c.c)
	return c, wtError(r)
}

func (s *Session) Scan(uri string) (*Scanner, error) {
	uriC := C.CString(uri)
	defer C.free(unsafe.Pointer(uriC))
	cfgC := C.CString("raw,readonly")
	defer C.free(unsafe.Pointer(cfgC))

	c := &Scanner{}
	r := C.wt_session_open_cursor(s.s, uriC, nil, cfgC, &c.c)
	return c, wtError(r)
}

type SyncMode string

const (
	SyncOff        = "off"
	SyncOn         = "on"
	SyncBackground = "background"
)

func (s *Session) LogFlush(sync SyncMode) error {
	cfgC := C.CString("sync=" + string(sync))
	defer C.free(unsafe.Pointer(cfgC))
	if r := C.wt_session_log_flush(s.s, cfgC); r != 0 {
		return wtError(r)
	}
	return nil
}

type TxCfg struct {
	Sync wtBool
}

func (s *Session) TxBegin(cfg ...TxCfg) error {
	cfgC := configC(cfg)
	defer C.free(unsafe.Pointer(cfgC))
	r := C.wt_session_begin_transaction(s.s, cfgC)
	s.inTx = (r == 0)
	return wtError(r)
}
func (s *Session) TxCommit(cfg ...TxCfg) error {
	cfgC := configC(cfg)
	defer C.free(unsafe.Pointer(cfgC))
	r := C.wt_session_commit_transaction(s.s, nil)
	s.inTx = false
	return wtError(r)
}
func (s *Session) TxRollback() error {
	r := C.wt_session_rollback_transaction(s.s, nil)
	s.inTx = false
	return wtError(r)
}
func (s *Session) InTx() bool {
	return s.inTx
}
