package wt

/*
#include <stdlib.h>
#include <wiredtiger.h>
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"reflect"
)

var (
	vTrue        = true
	vFalse       = false
	True   *bool = &vTrue
	False  *bool = &vFalse
)

type ErrorCode int

const (
	ErrNotFound ErrorCode = C.WT_NOTFOUND
)

type Error struct {
	Code ErrorCode
}

func (e *Error) Error() string {
	switch (e.Code) {
	case ErrNotFound:
		return "WT_NOTFOUND"
	default:
		return fmt.Sprintf("WTError: %v", e.Code)
	}
}

func ErrCode(e error) ErrorCode {
	wtErr, ok := e.(*Error)
	if !ok {
		return -1
	}
	return wtErr.Code
}

func wtError(errorCode C.int) error {
	return &Error{Code: ErrorCode(errorCode)}
}

func configC(config interface{}) *C.char {
	if config == nil || reflect.ValueOf(config).IsNil() {
		return nil
	}
	cfg, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	cfgC := C.CString(string(cfg))
	return cfgC
}
