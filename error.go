package wt

/*
#include <stdlib.h>
#include <wiredtiger.h>
*/
import "C"

import (
	"fmt"
)

type ErrorCode int

const (
	ErrNotFound ErrorCode = C.WT_NOTFOUND
)

type Error struct {
	Code ErrorCode
}

func (e *Error) Error() string {
	switch e.Code {
	case ErrNotFound:
		return "WT_NOTFOUND"
	default:
		return fmt.Sprintf("WTError: %d", e.Code)
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
	if errorCode == 0 {
		return nil
	}
	return &Error{Code: ErrorCode(errorCode)}
}
