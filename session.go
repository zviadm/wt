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
	if r := C.wt_session_close(s.s, nil); r != 0 {
		return wtError(r)
	}
	s.s = nil
	return nil
}

func (s *Session) IsClosed() bool {
	return s.s == nil
}

type DataSourceConfig struct {
	BlockCompressor string
}

func (s *Session) Create(name string, config *DataSourceConfig) error {
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	cfgC := configC(config)
	defer C.free(unsafe.Pointer(cfgC))

	if r := C.wt_session_create(s.s, nameC, cfgC); r != 0 {
		return wtError(r)
	}
	return nil
}

type DropConfig struct {
	Force       wtBool
	RemoveFiles wtBool
}

func (s *Session) Drop(name string, config *DropConfig) error {
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	cfgC := configC(config)
	defer C.free(unsafe.Pointer(cfgC))

	r := C.wt_session_drop(s.s, nameC, cfgC)
	return wtError(r)
}

type MutationConfig struct {
	Overwrite wtBool
	Bulk      wtBool
	raw       wtBool
}

func (s *Session) Mutate(uri string, config *MutationConfig) (*Mutator, error) {
	uriC := C.CString(uri)
	defer C.free(unsafe.Pointer(uriC))
	var cfgC *C.char
	if config != nil {
		config.raw = True // Always reading in "raw" mode.
		cfgC = configC(config)
	} else {
		cfgC = C.CString("raw")
	}
	defer C.free(unsafe.Pointer(cfgC))

	c := &Mutator{}
	r := C.wt_session_open_cursor(s.s, uriC, nil, cfgC, &c.c)
	return c, wtError(r)
}

// TODO: scan config?
func (s *Session) Scan(uri string) (*Scanner, error) {
	uriC := C.CString(uri)
	defer C.free(unsafe.Pointer(uriC))
	cfgC := C.CString("raw")
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

type TxConfig struct {
	Sync wtBool
}

func (s *Session) TxBegin(config *TxConfig) error {
	cfgC := configC(config)
	defer C.free(unsafe.Pointer(cfgC))
	r := C.wt_session_begin_transaction(s.s, cfgC)
	s.inTx = (r == 0)
	return wtError(r)
}
func (s *Session) TxCommit() error {
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
