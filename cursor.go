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
func (c *cursor) setKey(key []byte) {
	if len(key) > 0 {
		C.wt_cursor_set_key(c.c, unsafe.Pointer(&key[0]), C.size_t(len(key)))
	} else {
		C.wt_cursor_set_key(c.c, nil, 0)
	}
}
func (c *cursor) setValue(value []byte) {
	if len(value) > 0 {
		C.wt_cursor_set_value(c.c, unsafe.Pointer(&value[0]), C.size_t(len(value)))
	} else {
		C.wt_cursor_set_value(c.c, nil, 0)
	}
}

// Mutator exposes apis in a way to avoid any memory copies while inserting data.
// After each insert, cursor is immediatelly reset making it safe to directly pass in the
// pointers to the Go byte slices.
type Mutator struct {
	cursor
}

func (c *Mutator) Insert(key, value []byte) error {
	c.setKey(key)
	c.setValue(value)
	if r := C.wt_cursor_insert(c.c); r != 0 {
		return wtError(r)
	}
	return c.Reset()
}
func (c *Mutator) Update(key, value []byte) error {
	c.setKey(key)
	c.setValue(value)
	if r := C.wt_cursor_update(c.c); r != 0 {
		return wtError(r)
	}
	return c.Reset()
}
func (c *Mutator) Remove(key []byte) error {
	c.setKey(key)
	if r := C.wt_cursor_remove(c.c); r != 0 {
		return wtError(r)
	}
	return c.Reset()
}

type Scanner struct {
	cursor
}

const (
	// Go limits arrays to a length that will fit in a (signed) 32-bit integer.
	goArrayMaxLen = 0x7fffffff
)

// UnsafeKey() returns the current key referenced by the cursor. The memory
// is invalid after the next operation on the cursor.
func (c *Scanner) UnsafeKey() ([]byte, error) {
	var item C.WT_ITEM
	if r := C.wt_cursor_get_key(c.c, &item); r != 0 {
		return nil, wtError(r)
	}
	if item.size == 0 {
		return nil, nil
	}
	return (*[goArrayMaxLen]byte)(unsafe.Pointer(item.data))[:item.size:item.size], nil
}

// UnsafeValue() returns the current value referenced by the cursor. The memory
// is invalid after the next operation on the cursor.
func (c *Scanner) UnsafeValue() ([]byte, error) {
	var item C.WT_ITEM
	if r := C.wt_cursor_get_value(c.c, &item); r != 0 {
		return nil, wtError(r)
	}
	return (*[goArrayMaxLen]byte)(unsafe.Pointer(item.data))[:item.size:item.size], nil
}

// TODO(zviad): `cgo` overhead is most noticable for Scan calls when doing a range scan
// using next/prev. Might need to change the API to do bigger batch processing directly in C.
func (c *Scanner) Next() error {
	r := C.wt_cursor_next(c.c)
	return wtError(r)
}
func (c *Scanner) Prev() error {
	r := C.wt_cursor_prev(c.c)
	return wtError(r)
}
func (c *Scanner) Search(key []byte) error {
	c.setKey(key)
	r := C.wt_cursor_search(c.c)
	return wtError(r)
}

type NearMatchType int

const (
	MatchedExact   NearMatchType = 0
	MatchedSmaller NearMatchType = -1
	MatchedLarger  NearMatchType = 1
)

func (c *Scanner) SearchNear(key []byte) (NearMatchType, error) {
	var exact C.int
	c.setKey(key)
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

// Reads value for a specific key. Similar to UnsafeValue(), value returned becomes invalid
// after another operation is performed on the cursor, thus it needs to be consumed/copied immediatelly.
func (c *Scanner) ReadUnsafeValue(key []byte) ([]byte, error) {
	if err := c.Search(key); err != nil {
		return nil, err
	}
	return c.UnsafeValue()
}
