package logging

import (
	"io"
	"log"
)

var (
	Error *log.Logger
	Debug *log.Logger
	Trace *log.Logger
)

func Init(error io.Writer, debug io.Writer, trace io.Writer) {
	Error = log.New(error, "[Error] ", log.Lmicroseconds|log.Lshortfile)
	Debug = log.New(debug, "[Debug] ", log.Lmicroseconds|log.Lshortfile)
	Trace = log.New(trace, "[Trace] ", log.Lmicroseconds|log.Lshortfile)
}
