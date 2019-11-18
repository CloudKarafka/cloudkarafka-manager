package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
)

type Entry interface {
	ToLog() string
}

type CmdEntry struct {
	Cmd *exec.Cmd
}

var ignoreParams = []string{"-deststorepass", "-srcstorepass", "-destkeypass", "-password", "-storepass"}

func (me CmdEntry) ToLog() string {
	skipNext := false
	var buffer bytes.Buffer
	buffer.WriteString(me.Cmd.Path)
	for _, v := range me.Cmd.Args[1:] {
		if skipNext {
			buffer.WriteString(" *******")
		} else {
			buffer.WriteString(" " + v)
		}
		skipNext = false
		for _, skip := range ignoreParams {
			if v == skip {
				skipNext = true
			}
		}
	}
	return buffer.String()
}

type ErrorEntry struct {
	Err error
}

func (me ErrorEntry) ToLog() string {
	return me.Err.Error()
}

type MapEntry map[string]interface{}

func (me MapEntry) ToLog() string {
	keys := make([]string, len(me))
	i := 0
	for k := range me {
		keys[i] = k
		i += 1
	}
	sort.Strings(keys)
	var buffer bytes.Buffer
	for _, k := range keys {
		buffer.WriteString(fmt.Sprintf("%s=%v ", k, me[k]))
	}
	return buffer.String()
}

func Log(w io.Writer, level string, key string, data Entry) {
	fmt.Fprintf(w, "[%s] %s %s\n", strings.ToUpper(level), key, data.ToLog())
}

func Trace(key string, data Entry) {
	Log(os.Stderr, "trace", key, data)
}
func Debug(key string, data Entry) {
	Log(os.Stderr, "debug", key, data)
}
func Info(key string, data Entry) {
	Log(os.Stderr, "info", key, data)
}
func Warn(key string, data Entry) {
	Log(os.Stderr, "warn", key, data)
}
func Error(key string, data Entry) {
	Log(os.Stderr, "error", key, data)
}
