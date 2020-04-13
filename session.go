package wt

/*
#include <stdlib.h>
#include <wiredtiger.h>

// Expose WT methods accessed through function pointers:
int wt_session_close(
	WT_SESSION *session
	) {
    return session->close(session, NULL);
}
int wt_session_create(
	WT_SESSION *session,
	const char *name,
	_GoString_ config
	) {
    return session->create(session, name, _GoStringPtr(config));
}
int wt_session_drop(
	WT_SESSION *session,
	const char *name,
	_GoString_ config
	) {
    return session->drop(session, name, _GoStringPtr(config));
}
int wt_session_open_cursor(
	WT_SESSION *session,
	const char *uri,
	WT_CURSOR *to_dup,
	_GoString_ config,
	WT_CURSOR **cursorp
	) {
    return session->open_cursor(session, uri, to_dup, _GoStringPtr(config), cursorp);
}
int wt_session_log_flush(
	WT_SESSION *session,
	_GoString_ config
	) {
    return session->log_flush(session, _GoStringPtr(config));
}
int wt_session_begin_transaction(
	WT_SESSION *session,
	_GoString_ config
	) {
    return session->begin_transaction(session, _GoStringPtr(config));
}
int wt_session_commit_transaction(
	WT_SESSION *session,
	_GoString_ config
	) {
    return session->commit_transaction(session, _GoStringPtr(config));
}
int wt_session_rollback_transaction(
	WT_SESSION *session
	) {
    return session->rollback_transaction(session, NULL);
}
*/
import "C"

import (
	"unsafe"
)

// Session is a wrapper for WT_SESSION class.
type Session struct {
	s    *C.WT_SESSION
	inTx bool
}

// Close performs WT_SESSION:close call.
func (s *Session) Close() error {
	r := C.wt_session_close(s.s)
	s.s = nil
	if r != 0 {
		return wtError(r)
	}
	return nil
}

// Closed returns True if session has been explicitly closed using Close() call.
func (s *Session) Closed() bool {
	return s.s == nil
}

// DataSourceCfg mirrors options for WT_SESSION::create call.
type DataSourceCfg struct {
	BlockCompressor string
}

// Create performs WT_SESSION::create call.
func (s *Session) Create(name string, cfg ...DataSourceCfg) error {
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	cfgC := configC(cfg)
	if r := C.wt_session_create(s.s, nameC, cfgC); r != 0 {
		return wtError(r)
	}
	return nil
}

// DropCfg mirrors options for WT_SESSION::drop call.
type DropCfg struct {
	Force       wtBool
	RemoveFiles wtBool
}

// Drop performs WT_SESSION::drop call.
func (s *Session) Drop(name string, cfg ...DropCfg) error {
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	cfgC := configC(cfg)
	r := C.wt_session_drop(s.s, nameC, cfgC)
	return wtError(r)
}

// MutateCfg contains options that apply to write only cursor from
// WT_SESSION::open_cursor call.
type MutateCfg struct {
	Overwrite wtBool
	Bulk      wtBool
	raw       wtBool
}

// Mutate creates new write only cursor.
func (s *Session) Mutate(uri string, cfg ...MutateCfg) (*Mutator, error) {
	uriC := C.CString(uri)
	defer C.free(unsafe.Pointer(uriC))
	cfgC := "raw\x00"
	if len(cfg) >= 1 {
		cfg[0].raw = True
		cfgC = configC(cfg)
	}
	c := &Mutator{}
	r := C.wt_session_open_cursor(s.s, uriC, nil, cfgC, &c.c)
	return c, wtError(r)
}

// Scan creates new read only cursor.
func (s *Session) Scan(uri string) (*Scanner, error) {
	uriC := C.CString(uri)
	defer C.free(unsafe.Pointer(uriC))
	cfgC := "raw,readonly\x00"
	c := &Scanner{}
	r := C.wt_session_open_cursor(s.s, uriC, nil, cfgC, &c.c)
	return c, wtError(r)
}

// SyncMode describes different synchronization options.
type SyncMode string

// Synchronization options when manually flushing WiredTiger log.
const (
	SyncOff = "off"
	SyncOn  = "on"
	// SyncBackground = "background" (APIs for Background mode not implemented)
)

// LogFlush performs WT_SESSION::log_flush call.
func (s *Session) LogFlush(sync SyncMode) error {
	cfgC := "sync=" + string(sync) + "\x00"
	if r := C.wt_session_log_flush(s.s, cfgC); r != 0 {
		return wtError(r)
	}
	return nil
}

// TxCfg mirrors options for WT_SESSION::begin_transaction and
// WT_SESSION::commit_transaction calls.
type TxCfg struct {
	Sync wtBool
}

// TxBegin performs WT_SESSION::begin_transaction call.
func (s *Session) TxBegin(cfg ...TxCfg) error {
	cfgC := configC(cfg)
	r := C.wt_session_begin_transaction(s.s, cfgC)
	s.inTx = (r == 0)
	return wtError(r)
}

// TxCommit performs WT_SESSION::commit_transaction call.
func (s *Session) TxCommit(cfg ...TxCfg) error {
	cfgC := configC(cfg)
	r := C.wt_session_commit_transaction(s.s, cfgC)
	s.inTx = false
	return wtError(r)
}

// TxRollback performs WT_SESSION::rollback_transaction call.
func (s *Session) TxRollback() error {
	r := C.wt_session_rollback_transaction(s.s)
	s.inTx = false
	return wtError(r)
}

// InTx returns True, if transaction was started using TxBegin call and has not yet
// been finished by either TxCommit or TxRollback calls.
func (s *Session) InTx() bool {
	return !s.Closed() && s.inTx
}
