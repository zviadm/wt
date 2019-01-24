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
*/
import "C"

import (
	"unsafe"
)

type Session struct {
	s *C.WT_SESSION
}

func (s *Session) Close() error {
	if r := C.wt_session_close(s.s, nil); r != 0 {
		return wtError(r)
	}
	s.s = nil
	return nil
}

type DataSourceConfig struct {
	BlockCompressor string `json:"block_compressor,omitempty"`
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
	Force       *bool `json:"force,omitempty"`
	RemoveFiles *bool `json:"remove_files,omitempty"`
}

func (s *Session) Drop(name string, config *DropConfig) error {
	nameC := C.CString(name)
	defer C.free(unsafe.Pointer(nameC))
	cfgC := configC(config)
	defer C.free(unsafe.Pointer(cfgC))

	if r := C.wt_session_drop(s.s, nameC, cfgC); r != 0 {
		return wtError(r)
	}
	return nil
}

type MutationConfig struct {
	Overwrite *bool `json:"overwrite,omitempty"`
	Bulk      *bool `json:"bulk,omitempty"`
}

func (s *Session) Mutate(uri string, config *MutationConfig) (*Mutator, error) {
	uriC := C.CString(uri)
	defer C.free(unsafe.Pointer(uriC))
	cfgC := configC(config)
	defer C.free(unsafe.Pointer(cfgC))

	c := &Mutator{}
	if r := C.wt_session_open_cursor(s.s, uriC, nil, cfgC, &c.c); r != 0 {
		return nil, wtError(r)
	}
	return c, nil
}

// TODO: scan config?
func (s *Session) Scan(uri string) (*Scanner, error) {
	uriC := C.CString(uri)
	defer C.free(unsafe.Pointer(uriC))

	c := &Scanner{}
	if r := C.wt_session_open_cursor(s.s, uriC, nil, nil, &c.c); r != 0 {
		return nil, wtError(r)
	}
	return c, nil
}