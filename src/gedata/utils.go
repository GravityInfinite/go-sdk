package gedata

import (
	"fmt"
	"os"
	"reflect"
	"regexp"
	"time"
)

const (
	DATE_FORMAT = "2006-01-02 15:04:05.000"
	KEY_PATTERN = "^[a-zA-Z$][A-Za-z0-9_]{0,49}$"
)

// A string of 50 letters and digits that sgerts with '#' or a letter
var keyPattern, _ = regexp.Compile(KEY_PATTERN)

func mergeProperties(target, source map[string]interface{}) {
	for k, v := range source {
		target[k] = v
	}
}

func extractTime(p map[string]interface{}) string {
	if t, ok := p["#time"]; ok {
		delete(p, "#time")
		switch v := t.(type) {
		case string:
			return v
		case time.Time:
			return v.Format(DATE_FORMAT)
		default:
			return time.Now().Format(DATE_FORMAT)
		}
	}

	return time.Now().Format(DATE_FORMAT)
}

func extractStringProperty(p map[string]interface{}, key string) string {
	if t, ok := p[key]; ok {
		delete(p, key)
		v, ok := t.(string)
		if !ok {
			fmt.Fprintln(os.Stderr, "Invalid data type for "+key)
		}
		return v
	}
	return ""
}

func isNotNumber(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
	case float32, float64:
	default:
		return true
	}
	return false
}

func isNotArrayOrSlice(v interface{}) bool {
	typeOf := reflect.TypeOf(v)
	switch typeOf.Kind() {
	case reflect.Array:
	case reflect.Slice:
	default:
		return true
	}
	return false
}

func checkPattern(name []byte) bool {
	return keyPattern.Match(name)
}
