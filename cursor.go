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

void _cursor_set_key(WT_CURSOR *cursor, const void *data, size_t size) {
	WT_ITEM item;
	item.data = data;
	item.size = size;
	return cursor->set_key(cursor, &item);
}
void _cursor_set_value(WT_CURSOR *cursor, const void *data, size_t size) {
	WT_ITEM item;
	item.data = data;
	item.size = size;
	return cursor->set_value(cursor, &item);
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
int wt_cursor_search(
	WT_CURSOR *cursor,
	const void *key, size_t key_size) {
	_cursor_set_key(cursor, key, key_size);
    return cursor->search(cursor);
}
int wt_cursor_search_near(
	WT_CURSOR *cursor,
	const void *key, size_t key_size,
	int *exactp) {
	_cursor_set_key(cursor, key, key_size);
    return cursor->search_near(cursor, exactp);
}
int wt_cursor_remove(WT_CURSOR *cursor) {
	return cursor->remove(cursor);
}
int wt_cursor_update(
	WT_CURSOR *cursor,
	const void *value, size_t value_size) {
	_cursor_set_value(cursor, value, value_size);
    return cursor->update(cursor);
}

int wt_cursor_insert(
	WT_CURSOR *cursor,
	const void *key, size_t key_size,
	const void *value, size_t value_size) {
	_cursor_set_key(cursor, key, key_size);
	_cursor_set_value(cursor, value, value_size);
    return cursor->insert(cursor);
}
int wt_cursor_update_and_reset(
	WT_CURSOR *cursor,
	const void *key, size_t key_size,
	const void *value, size_t value_size) {
	_cursor_set_key(cursor, key, key_size);
	_cursor_set_value(cursor, value, value_size);
    int r = cursor->update(cursor);
	if (r != 0) {
		return r;
	}
	return cursor->reset(cursor);
}
int wt_cursor_remove_and_reset(
	WT_CURSOR *cursor,
	const void *key, size_t key_size) {
	_cursor_set_key(cursor, key, key_size);
	int r = cursor->remove(cursor);
	if (r != 0) {
		return r;
	}
	return cursor->reset(cursor);
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

// Cursor is a wrapper for WT_CURSOR class. Cursor exposes WT_CURSOR methods in a way
// to make it safe and efficient for Go<->CGO integration.
type Cursor struct {
	c *C.WT_CURSOR
}

// Close performs WT_CURSOR::close call.
func (c *Cursor) Close() error {
	r := C.wt_cursor_close(c.c)
	c.c = nil
	return wtError(r)
}

// Reset performs WT_CURSOR::reset call.
func (c *Cursor) Reset() error {
	r := C.wt_cursor_reset(c.c)
	return wtError(r)
}

// UnsafeKey returns date returned by WT_CURSOR::get_key call. This call doesn't copy the data
// that it reads from `C` memory. Thus, the byte slice returned by this function is only valid until
// next operation on the cursor, or until session is closed.
func (c *Cursor) UnsafeKey() ([]byte, error) {
	var item C.WT_ITEM
	if r := C.wt_cursor_get_key(c.c, &item); r != 0 {
		return nil, wtError(r)
	}
	return (*[goArrayMaxLen]byte)(item.data)[:item.size:item.size], nil
}

// Key returns copy of data returned by WT_CURSOR::get_key call.
func (c *Cursor) Key() ([]byte, error) {
	r, err := c.UnsafeKey()
	return copyBuffer(r), err
}

// UnsafeValue returns date returned by WT_CURSOR::get_value call. This call doesn't copy the data
// that it reads from `C` memory. Thus, the byte slice returned by this function is only valid until
// next operation on the cursor, or until session is closed.
func (c *Cursor) UnsafeValue() ([]byte, error) {
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
func (c *Cursor) Value() ([]byte, error) {
	r, err := c.UnsafeValue()
	return copyBuffer(r), err
}

// Next performs WT_CURSOR::next call.
func (c *Cursor) Next() error {
	r := C.wt_cursor_next(c.c)
	return wtError(r)
}

// Prev performs WT_CURSOR::prev call.
func (c *Cursor) Prev() error {
	r := C.wt_cursor_prev(c.c)
	return wtError(r)
}

// Search performs WT_CURSOR::search call.
func (c *Cursor) Search(key []byte) error {
	keyP := unsafe.Pointer(&key[0])
	r := C.wt_cursor_search(c.c, keyP, C.size_t(len(key)))
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
func (c *Cursor) SearchNear(key []byte) (NearMatchType, error) {
	var exact C.int
	keyP := unsafe.Pointer(&key[0])
	r := C.wt_cursor_search_near(c.c, keyP, C.size_t(len(key)), &exact)
	if r != 0 {
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
func (c *Cursor) ReadUnsafeValue(key []byte) ([]byte, error) {
	if err := c.Search(key); err != nil {
		return nil, err
	}
	return c.UnsafeValue()
}

// ReadValue returns copy of value for a specific key. Resets cursor afterwards to
// keep it in a clean state.
func (c *Cursor) ReadValue(key []byte) ([]byte, error) {
	r, err := c.ReadUnsafeValue(key)
	if err != nil {
		return nil, err
	}
	if err := c.Reset(); err != nil {
		return nil, err
	}
	return copyBuffer(r), nil
}

// Remove removes element that cursor is point to
// using WT_CURSOR::remove call.
func (c *Cursor) Remove() error {
	r := C.wt_cursor_remove(c.c)
	return wtError(r)
}

// Update updates value of element that cursor is pointing to
// using WT_CURSOR::update call.
func (c *Cursor) Update(value []byte) error {
	var valueP unsafe.Pointer
	if len(value) > 0 {
		valueP = unsafe.Pointer(&value[0])
	}
	r := C.wt_cursor_update(c.c, valueP, C.size_t(len(value)))
	return wtError(r)
}

// Insert performs WT_CURSOR::insert call. Cursor is reset after this call.
func (c *Cursor) Insert(key, value []byte) error {
	keyP := unsafe.Pointer(&key[0]) // Defining here, avoids allocation :O
	var valueP unsafe.Pointer
	if len(value) > 0 {
		valueP = unsafe.Pointer(&value[0])
	}
	r := C.wt_cursor_insert(
		c.c, keyP, C.size_t(len(key)), valueP, C.size_t(len(value)))
	return wtError(r)
}

// RemoveKey performs WT_CURSOR::remove call. Cursor is reset after this call.
func (c *Cursor) RemoveKey(key []byte) error {
	keyP := unsafe.Pointer(&key[0])
	r := C.wt_cursor_remove_and_reset(c.c, keyP, C.size_t(len(key)))
	return wtError(r)
}

// UpdateValue performs WT_CURSOR::update call. Cursor is reset after this call.
func (c *Cursor) UpdateValue(key, value []byte) error {
	keyP := unsafe.Pointer(&key[0])
	var valueP unsafe.Pointer
	if len(value) > 0 {
		valueP = unsafe.Pointer(&value[0])
	}
	r := C.wt_cursor_update_and_reset(
		c.c, keyP, C.size_t(len(key)), valueP, C.size_t(len(value)))
	return wtError(r)
}

func copyBuffer(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	r := make([]byte, len(in))
	copy(r, in)
	return r
}
