package wt

import "C"

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type wtBool int

// Boolean configuration values.
const (
	Default wtBool = 0
	False   wtBool = 1
	True    wtBool = 2
)

// Bool converts regular bool type to wtBool.
func Bool(v bool) wtBool {
	if v {
		return True
	}
	return False
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func toSnakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

// Encodes config to wiredtiger configuration string.
// config must be []ConfigStruct of type, with length of 0 or 1,
// otherwise this function will panic.
// Transforms CamelCase config fields to snake_case and skips fields
// with default values.
func configC(config interface{}) *C.char {
	v := reflect.ValueOf(config)
	if config == nil || v.IsNil() || v.Len() == 0 {
		return nil
	}
	if v.Len() > 1 {
		panic("only 1 config struct must be passed")
	}
	v = v.Index(0)
	vt := v.Type()
	cfgParts := make([]string, 0, vt.NumField())
	for idx := 0; idx < vt.NumField(); idx++ {
		vf := vt.Field(idx)
		name := toSnakeCase(vf.Name)

		vv := v.Field(idx)
		if vv.Kind() == reflect.Ptr {
			if vv.IsNil() {
				continue
			}
			vv = reflect.Indirect(vv)
		}
		switch vv.Kind() {
		case reflect.Int:
			if vv.Int() == 0 {
				break
			}
			vvv := vv.Int()
			if vf.Type.Name() == "wtBool" {
				vvv--
			}
			cfgParts = append(cfgParts, name+"="+strconv.Itoa(int(vvv)))
			break
		case reflect.String:
			vvv := vv.String()
			if vvv == "" {
				break
			}
			vvv = strings.ReplaceAll(vvv, "\"", "\\\"")
			cfgParts = append(cfgParts, name+"=\""+vvv+"\"")
			break
		case reflect.Slice:
			vvv := make([]string, vv.Len())
			for idx := range vvv {
				vvv[idx] = vv.Index(idx).String()
				vvv[idx] = strings.ReplaceAll(vvv[idx], "\"", "\\\"")
			}
			cfgParts = append(cfgParts, name+"=("+strings.Join(vvv, ",")+")")
			break
		default:
			panic(fmt.Sprintf("unsupported type: %s:%s", vf.Name, vv.Kind()))
		}
	}
	cfg := strings.Join(cfgParts, ",")
	cfgC := C.CString(cfg)
	return cfgC
}
