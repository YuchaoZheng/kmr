package log

import (
	"io"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"fmt"
)

var (
	l *dsLogger = nil
)

const (
	LevelFatal = iota
	LevelPanic = iota
	LevelError = iota
	LevelInfo  = iota
	LevelDebug = iota
)

const (
	prefixDebug = "[DEBUG]"
	prefixInfo  = "[INFO]"
	prefixError = "[ERROR]"
	prefixPanic = "[PANIC]"
	prefixFatal = "[FATAL]"
)

type dsLogger struct {
	level  int
	debugL *log.Logger
	infoL  *log.Logger
	errorL *log.Logger
	panicL *log.Logger
	fatalL *log.Logger
}

func InitLog(prefix string, logfileName string, level int) {
	if l != nil {
		return
	}
	l = new(dsLogger)
	l.level = level
	mw := io.MultiWriter(os.Stdout)
	if logfileName != "" {
		f, err := os.OpenFile(logfileName, os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			log.Fatal(err)
		}
		mw = io.MultiWriter(os.Stdout, f)
	}
	if level >= LevelDebug {
		l.debugL = log.New(mw, prefix+prefixDebug, log.Ldate|log.Lmicroseconds)
	}
	if level >= LevelInfo {
		l.infoL = log.New(mw, prefix+prefixInfo, log.Ldate|log.Lmicroseconds)
	}
	if level >= LevelError {
		l.errorL = log.New(mw, prefix+prefixError, log.Ldate|log.Lmicroseconds)
	}
	if level >= LevelPanic {
		l.panicL = log.New(mw, prefix+prefixPanic, log.Ldate|log.Lmicroseconds)
	}
	if level >= LevelFatal {
		l.fatalL = log.New(mw, prefix+prefixFatal, log.Ldate|log.Lmicroseconds)
	}
}

func GetInfoLogger() *log.Logger {
	return l.infoL
}
func Debug(v ...interface{}) {
	if l == nil {
		log.Println(v...)
		return
	}
	if l.level < LevelDebug {
		return
	}
	l.debugL.Println(v...)
}

func Debugf(format string, v ...interface{}) {
	if l == nil {
		log.Printf(format, v...)
		return
	}
	if l.level < LevelDebug {
		return
	}
	l.debugL.Printf(format, v...)
}

func Info(v ...interface{}) {
	if l == nil {
		log.Println(v...)
		return
	}
	if l.level < LevelInfo {
		return
	}
	l.infoL.Println(v...)
}

func Infof(format string, v ...interface{}) {
	if l == nil {
		log.Printf(format, v...)
		return
	}
	if l.level < LevelInfo {
		return
	}
	l.infoL.Printf(format, v...)

}

func Error(v ...interface{}) {
	if l == nil {
		log.Println(v...)
		return
	}
	fs := filePath(l.errorL)
	l.errorL.Println(append([]interface{}{fs}, v...)...)
}

func Errorf(format string, v ...interface{}) {
	if l == nil {
		log.Printf(format, v...)
		return
	}
	fs := filePath(l.errorL)
	l.errorL.Printf("%s "+format, append([]interface{}{fs}, v...)...)
}

func Fatal(v ...interface{}) {
	if l == nil {
		log.Println(v...)
	} else {
		fs := filePath(l.errorL)
		l.fatalL.Println(append([]interface{}{fs}, v...)...)
	}
	panic(fmt.Sprintln(v...))
}

func Panic(v interface{}) {
	if l == nil {
		log.Println(v)
		log.Println(string(debug.Stack()))
	} else {
		l.panicL.Println(v)
		l.panicL.Println(string(debug.Stack()))
	}
	panic(v)
}

func Fatalf(format string, v ...interface{}) {
	if l == nil {
		log.Printf(format, v...)
	} else {
		l.fatalL.Printf(format, v...)
	}
	panic(fmt.Sprintf(format, v...))
}

func filePath(clog *log.Logger) string {
	var ok bool
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "???"
		line = 0
	}
	return file + ":" + strconv.Itoa(line) + ":"
}
