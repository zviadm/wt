package wt

import "C"

import (
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type wtBool int

const (
	Default wtBool = 0
	False   wtBool = 1
	True    wtBool = 2
)

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

func configC(config interface{}) *C.char {
	v := reflect.ValueOf(config)
	if config == nil || v.IsNil() {
		return nil
	}
	v = reflect.Indirect(v)
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
				continue
			}
			vvv := vv.Int()
			if vf.Type.Name() == "wtBool" {
				vvv -= 1
			}
			cfgParts = append(cfgParts, name+"="+strconv.Itoa(int(vvv)))
			continue
		case reflect.String:
			vvv := vv.String()
			if vvv == "" {
				continue
			}
			// TODO(zviad): escape `s`
			cfgParts = append(cfgParts, name+"=\""+vvv+"\"")
			continue
		default:
			panic(fmt.Sprintf("unsupport type: %v:%v", vf.Name, vv.Kind()))
		}
	}
	cfg := strings.Join(cfgParts, ",")
	log.Printf("cfg: %s", cfg)
	cfgC := C.CString(cfg)
	return cfgC
}
