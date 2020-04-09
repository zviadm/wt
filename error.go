package wt

/*
#include <stdlib.h>
#include <wiredtiger.h>
*/
import "C"

// ErrorCode enum describes WiredTiger-specific error codes.
type ErrorCode int

// WiredTiger specific error codes.
const (
	ErrRollback        ErrorCode = C.WT_ROLLBACK
	ErrDuplicateKey    ErrorCode = C.WT_DUPLICATE_KEY
	ErrError           ErrorCode = C.WT_ERROR
	ErrNotFound        ErrorCode = C.WT_NOTFOUND
	ErrPanic           ErrorCode = C.WT_PANIC
	ErrRunRecover      ErrorCode = C.WT_RUN_RECOVERY
	ErrCacheAll        ErrorCode = C.WT_CACHE_FULL
	ErrPrepareConflict ErrorCode = C.WT_PREPARE_CONFLICT
	ErrTrySalvage      ErrorCode = C.WT_TRY_SALVAGE
)

// Error describes WiredTiger error.
type Error struct {
	Code ErrorCode
}

func (e *Error) Error() string {
	return C.GoString(C.wiredtiger_strerror(C.int(e.Code)))
}

// ErrCode extracts WiredTiger error code from any error. If input is not
// WiredTiger error, it will return ErrError.
func ErrCode(e error) ErrorCode {
	wtErr, ok := e.(*Error)
	if !ok {
		return ErrError
	}
	return wtErr.Code
}

func wtError(errorCode C.int) error {
	if errorCode == 0 {
		return nil
	}
	return &Error{Code: ErrorCode(errorCode)}
}
