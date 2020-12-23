package errs

import (
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"unsafe"
)

const (
	DEBUG = iota
	WARN
	PANIC
	FATAL
)

var (
	Mode = DEBUG
)

type myErr struct {
	file string
	line int
	msgs []interface{}
}

func init() {

}

const (
	LOGIC_ERR = "interal logic error!"
)

func New(msgs ...interface{}) error {
	e := &myErr{}
	e.msgs = msgs

	if Mode >= DEBUG {
		if _, file, line, ok := runtime.Caller(1); ok {
			e.file = fileName(file)
			e.line = line
		}
	}
	return e
}
func Logic() error {
	e := &myErr{msgs: []interface{}{LOGIC_ERR}}
	if _, file, line, ok := runtime.Caller(1); ok {
		e.file = fileName(file)
		e.line = line
	}
	return e
}
func NotImplemented() error {
	e := &myErr{msgs: []interface{}{"not implemented!"}}
	if _, file, line, ok := runtime.Caller(1); ok {
		e.file = fileName(file)
		e.line = line
	}
	return e
}
func (this *myErr) Error() string {
	l := []interface{}{this.file, "[", this.line, "]:"}
	l = append(l, this.msgs...)
	return fmt.Sprint(l...)
}
func fileName(name string) string {

	if pos := strings.LastIndex(name, "/"); pos >= 0 {
		return name[pos+1:]
	} else {
		return name
	}
}
func Println(v ...interface{}) {
	empty := true
	for _, i := range v {
		if i != nil {
			empty = false
			break
		}
	}
	if empty {
		return
	}
	if Mode <= DEBUG {
		if _, file, line, ok := runtime.Caller(1); ok {
			fmt.Printf("%s->%d ", fileName(file), line)
		}
	}
	fmt.Println(v...)
}
func Warn(v ...interface{}) {
	if Mode <= WARN {
		if _, file, line, ok := runtime.Caller(1); ok {
			fmt.Printf("%s->%d ", fileName(file), line)
		}
	}
	fmt.Println(v...)
}

func Panic(v ...interface{}) {
	if Mode <= PANIC {
		if _, file, line, ok := runtime.Caller(1); ok {
			fmt.Printf("%s->%d ", fileName(file), line)
		}
	}
	fmt.Println(v...)
}
func Fatal(v ...interface{}) {
	if Mode <= FATAL {
		if _, file, line, ok := runtime.Caller(1); ok {
			fmt.Printf("%s->%d ", fileName(file), line)
		}
	}
	fmt.Println(v...)
}
func SetTo(dst *error, src error) bool {
	if src != nil {
		return atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(dst)), nil, unsafe.Pointer(&src))
	} else {
		return false
	}
}
func GetFrom(dst *error) error {
	if t := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(dst))); t == nil {
		return nil
	} else {
		return *(*error)(t)
	}
}
