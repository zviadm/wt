package wt

/*
#include <stdlib.h>
#include <wiredtiger.h>
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	vTrue        = true
	vFalse       = false
	True   *bool = &vTrue
	False  *bool = &vFalse
)

func wtError(errorCode C.int) error {
	// TODO: do better
	return errors.New(fmt.Sprintf("WTError: %v", errorCode))
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
