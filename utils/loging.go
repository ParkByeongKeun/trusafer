package utils

import (
	"io"
	"log"
	"os"
)

type Logger struct {
	Title *log.Logger
	Info  *log.Logger
	Error *log.Logger
}

func NewLogger(fpLog io.Writer) Logger {
	var logger Logger
	var writer io.Writer

	if fpLog != nil {
		writer = io.MultiWriter(fpLog)
		// writer = io.MultiWriter(fpLog, os.Stdout)
	} else {
		writer = io.Writer(os.Stdout)
	}
	logger.Title = log.New(writer, "[TITLE] ", log.Ldate|log.Ltime|log.Lmsgprefix)
	logger.Info = log.New(writer, "[INFO ]  - ", log.Ldate|log.Ltime|log.Lmsgprefix)
	logger.Error = log.New(writer, "[ERROR]  - ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmsgprefix)

	return logger
}
