package wt

/*
#include <stdlib.h>
#include <wiredtiger.h>

// Expose WT methods accessed through function pointers:
int wt_cursor_close(WT_CURSOR *cursor) {
    return cursor->close(cursor);
}
int wt_cursor_reset(WT_CURSOR *cursor) {
    return cursor->reset(cursor);
}

void wt_cursor_set_key(WT_CURSOR *cursor, const void *data, size_t size) {
	WT_ITEM item;
	item.data = data;
	item.size = size;
	return cursor->set_key(cursor, &item);
}
void wt_cursor_set_value(WT_CURSOR *cursor, const void *data, size_t size) {
	WT_ITEM item;
	item.data = data;
	item.size = size;
    return cursor->set_value(cursor, &item);
}

int wt_cursor_insert(WT_CURSOR *cursor) {
    return cursor->insert(cursor);
}
int wt_cursor_update(WT_CURSOR *cursor) {
    return cursor->update(cursor);
}
int wt_cursor_remove(WT_CURSOR *cursor) {
    return cursor->remove(cursor);
}

int wt_cursor_get_key(WT_CURSOR *cursor, WT_ITEM *item) {
	return cursor->get_key(cursor, item);
}
int wt_cursor_get_value(WT_CURSOR *cursor, WT_ITEM *item) {
	return cursor->get_value(cursor, item);
}

int wt_cursor_next(WT_CURSOR *cursor) {
    return cursor->next(cursor);
}
int wt_cursor_prev(WT_CURSOR *cursor) {
    return cursor->prev(cursor);
}
int wt_cursor_search(WT_CURSOR *cursor) {
    return cursor->search(cursor);
}
int wt_cursor_search_near(WT_CURSOR *cursor, int *exactp) {
    return cursor->search_near(cursor, exactp);
}

*/
import "C"

import (
	"unsafe"
)

const (
	// Go limits arrays to a length that will fit in a (signed) 32-bit integer. This is used
	// to read cursor values without making copies.
	goArrayMaxLen = 0x7fffffff
)

// HAX taken from runtime/stubs.go file. Without this trick, unsafe.Pointer calls in
// cursor_set_key & cursor_set_value calls would cause an unnecessary memory allocation.
//go:nosplit
func noescape(p unsafe.Pointer) unsafe.Pointer {
	x := uintptr(p)
	return unsafe.Pointer(x ^ 0)
}

type cursor struct {
	c *C.WT_CURSOR
}

func (c *cursor) Close() error {
	r := C.wt_cursor_close(c.c)
	c.c = nil
	return wtError(r)
}
func (c *cursor) Reset() error {
	r := C.wt_cursor_reset(c.c)
	return wtError(r)
}

// Mutator exposes WT_CURSOR mutation apis in a way that avoids any memory copies while
// inserting data. After each operation, cursor is immediatelly reset, making it safe to
// directly pass in Go byte slices.
type Mutator struct {
	cursor
}

// Insert performs WT_CURSOR::insert call.
func (c *Mutator) Insert(key, value []byte) error {
	C.wt_cursor_set_key(c.c, noescape(unsafe.Pointer(&key[0])), C.size_t(len(key)))
	if len(value) == 0 {
		C.wt_cursor_set_value(c.c, nil, 0)
	} else {
		C.wt_cursor_set_value(c.c, noescape(unsafe.Pointer(&value[0])), C.size_t(len(value)))
	}
	// Cursor automatically gets reset when using `insert`.
	r := C.wt_cursor_insert(c.c)
	return wtError(r)
}

// Update performs WT_CURSOR::update call.
func (c *Mutator) Update(key, value []byte) error {
	C.wt_cursor_set_key(c.c, noescape(unsafe.Pointer(&key[0])), C.size_t(len(key)))
	if len(value) == 0 {
		C.wt_cursor_set_value(c.c, nil, 0)
	} else {
		C.wt_cursor_set_value(c.c, noescape(unsafe.Pointer(&value[0])), C.size_t(len(value)))
	}
	if r := C.wt_cursor_update(c.c); r != 0 {
		return wtError(r)
	}
	return c.Reset()
}

// Remove performs WT_CURSOR::remove call.
func (c *Mutator) Remove(key []byte) error {
	C.wt_cursor_set_key(c.c, noescape(unsafe.Pointer(&key[0])), C.size_t(len(key)))
	if r := C.wt_cursor_remove(c.c); r != 0 {
		return wtError(r)
	}
	return c.Reset()
}

// Scanner exposes WT_CURSOR read apis.
type Scanner struct {
	cursor
}

// UnsafeKey returns date returned by WT_CURSOR::get_key call. This call doesn't copy the data
// that it reads from `C` memory. Thus, the byte slice returned by this function is only valid until
// next operation on the cursor, or until session is closed.
func (c *Scanner) UnsafeKey() ([]byte, error) {
	var item C.WT_ITEM
	if r := C.wt_cursor_get_key(c.c, &item); r != 0 {
		return nil, wtError(r)
	}
	return (*[goArrayMaxLen]byte)(item.data)[:item.size:item.size], nil
}

// Key returns copy of data returned by WT_CURSOR::get_key call.
func (c *Scanner) Key() ([]byte, error) {
	r, err := c.UnsafeKey()
	return copyBuffer(r), err
}

// UnsafeValue returns date returned by WT_CURSOR::get_value call. This call doesn't copy the data
// that it reads from `C` memory. Thus, the byte slice returned by this function is only valid until
// next operation on the cursor, or until session is closed.
func (c *Scanner) UnsafeValue() ([]byte, error) {
	var item C.WT_ITEM
	if r := C.wt_cursor_get_value(c.c, &item); r != 0 {
		return nil, wtError(r)
	}
	if item.size == 0 {
		return nil, nil
	}
	return (*[goArrayMaxLen]byte)(unsafe.Pointer(item.data))[:item.size:item.size], nil
}

// Value returns copy of data returned by WT_CURSOR::get_value call.
func (c *Scanner) Value() ([]byte, error) {
	r, err := c.UnsafeValue()
	return copyBuffer(r), err
}

// Next performs WT_CURSOR::next call.
// TODO(zviad): `cgo` overhead is most noticable for Scan calls when doing a range scan
// using next/prev. Might need to change the API to do bigger batch processing directly in C.
func (c *Scanner) Next() error {
	r := C.wt_cursor_next(c.c)
	return wtError(r)
}

// Prev performs WT_CURSOR::prev call.
func (c *Scanner) Prev() error {
	r := C.wt_cursor_prev(c.c)
	return wtError(r)
}

// Search performs WT_CURSOR::search call.
func (c *Scanner) Search(key []byte) error {
	C.wt_cursor_set_key(c.c, noescape(unsafe.Pointer(&key[0])), C.size_t(len(key)))
	r := C.wt_cursor_search(c.c)
	return wtError(r)
}

// NearMatchType describes type of match that is found with SearchNear call.
type NearMatchType int

// Match types that SearchNear call can return.
const (
	MatchedExact   NearMatchType = 0
	MatchedSmaller NearMatchType = -1
	MatchedLarger  NearMatchType = 1
)

// SearchNear performs WT_CURSOR::search_near call.
func (c *Scanner) SearchNear(key []byte) (NearMatchType, error) {
	var exact C.int
	C.wt_cursor_set_key(c.c, noescape(unsafe.Pointer(&key[0])), C.size_t(len(key)))
	if r := C.wt_cursor_search_near(c.c, &exact); r != 0 {
		return 0, wtError(r)
	}
	if exact < 0 {
		return MatchedSmaller, nil
	} else if exact > 0 {
		return MatchedLarger, nil
	} else {
		return MatchedExact, nil
	}
}

// ReadUnsafeValue returns value for a specific key. Similar to UnsafeValue, it doesn't copy the data
// from `C` memory, thus it is only valid until next operation on cursor (or until session is closed).
func (c *Scanner) ReadUnsafeValue(key []byte) ([]byte, error) {
	if err := c.Search(key); err != nil {
		return nil, err
	}
	return c.UnsafeValue()
}

// ReadValue returns copy of value for a specific key. Resets cursor afterwards to
// keep it in a clean state.
func (c *Scanner) ReadValue(key []byte) ([]byte, error) {
	r, err := c.ReadUnsafeValue(key)
	if err != nil {
		return nil, err
	}
	if err := c.Reset(); err != nil {
		return nil, err
	}
	return copyBuffer(r), nil
}

func copyBuffer(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	r := make([]byte, len(in))
	copy(r, in)
	return r
}
