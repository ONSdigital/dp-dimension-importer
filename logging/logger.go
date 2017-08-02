package logging

import (
	"io"
	"log"
	"strings"
	"os"
	"io/ioutil"
)

const (
	TraceLevel = "trace"
	DebugLevel = "debug"
	ErrorLevel = "error"
)

var (
	Error *log.Logger
	Debug *log.Logger
	Trace *log.Logger

	traceWriter io.Writer
	debugWriter io.Writer
	errorWriter io.Writer
)

func Init(lvl string) {
	if len(lvl) == 0 {
		lvl = ErrorLevel
	}
	lvl = strings.ToLower(lvl)

	switch lvl {
	case TraceLevel:
		traceWriter = os.Stdout
		debugWriter = os.Stdout
		errorWriter = os.Stdout
		break
	case DebugLevel:
		traceWriter = ioutil.Discard
		debugWriter = os.Stdout
		errorWriter = os.Stdout
		break
	default:
		traceWriter = ioutil.Discard
		debugWriter = ioutil.Discard
		errorWriter = os.Stdout
	}

	Trace = log.New(traceWriter, "[Trace] ", log.Lmicroseconds|log.Lshortfile)
	Debug = log.New(debugWriter, "[Debug] ", log.Lmicroseconds|log.Lshortfile)
	Error = log.New(errorWriter, "[Error] ", log.Lmicroseconds|log.Lshortfile)
}
