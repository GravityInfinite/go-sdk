package gedata

import (
	"fmt"
	"time"
)

// SDK_LOG_PREFIX const
const SDK_LOG_PREFIX = "[ge data]"

var logInsgence GELogger

type GELogLevel int32

const (
	GELogLevelOff     GELogLevel = 1
	GELogLevelError   GELogLevel = 2
	GELogLevelWarning GELogLevel = 3
	GELogLevelInfo    GELogLevel = 4
	GELogLevelDebug   GELogLevel = 5
)

// default is GELogLevelOff
var currentLogLevel = GELogLevelOff

// GELogger User-defined log classes must comply with interface
type GELogger interface {
	Print(message string)
}

// SetLogLevel Set the log output level
func SetLogLevel(level GELogLevel) {
	if level < GELogLevelOff || level > GELogLevelDebug {
		fmt.Println(SDK_LOG_PREFIX + "log type error")
		return
	} else {
		currentLogLevel = level
	}
}

// SetCustomLogger Set a custom log input class, usually you don't need to set it up.
func SetCustomLogger(logger GELogger) {
	if logger != nil {
		logInsgence = logger
	}
}

func geLog(level GELogLevel, format string, v ...interface{}) {
	if level > currentLogLevel {
		return
	}

	var modeStr string
	switch level {
	case GELogLevelError:
		modeStr = "[Error] "
		break
	case GELogLevelWarning:
		modeStr = "[Warning] "
		break
	case GELogLevelInfo:
		modeStr = "[Info] "
		break
	case GELogLevelDebug:
		modeStr = "[Debug] "
		break
	default:
		modeStr = "[Info] "
		break
	}

	if logInsgence != nil {
		msg := fmt.Sprintf(SDK_LOG_PREFIX+modeStr+format+"\n", v...)
		logInsgence.Print(msg)
	} else {
		logTime := fmt.Sprintf("[%v]", time.Now().Format("2006-01-02 15:04:05.000"))
		fmt.Printf(logTime+SDK_LOG_PREFIX+modeStr+format+"\n", v...)
	}
}

func geLogDebug(format string, v ...interface{}) {
	geLog(GELogLevelDebug, format, v...)
}

func geLogInfo(format string, v ...interface{}) {
	geLog(GELogLevelInfo, format, v...)
}

func geLogError(format string, v ...interface{}) {
	geLog(GELogLevelError, format, v...)
}

func geLogWarning(format string, v ...interface{}) {
	geLog(GELogLevelWarning, format, v...)
}

// Deprecated: please use gedata.SetLogLevel(gedata.GELogLevelOff)
type LogType int32

// Deprecated: please use gedata.SetLogLevel(gedata.GELogLevelOff)
const (
	LoggerTypeOff               LogType = 1 << 0                                // disable log
	LoggerTypePrint             LogType = 1 << 1                                // print on console
	LoggerTypeWriteFile         LogType = 1 << 2                                // print to file
	LoggerTypePrintAndWriteFile         = LoggerTypePrint | LoggerTypeWriteFile // print both on console and file
)

// Deprecated: please use gedata.SetLogLevel(gedata.GELogLevelOff)
type LoggerConfig struct {
	Type LogType
	Path string
}

// Deprecated: please use gedata.SetLogLevel(gedata.GELogLevelOff)
func SetLoggerConfig(config LoggerConfig) {
	if config.Type < LoggerTypeOff || config.Type > LoggerTypePrintAndWriteFile {
		fmt.Println(SDK_LOG_PREFIX + "log type error")
		return
	}
	if config.Type&LoggerTypeOff == LoggerTypeOff {
		currentLogLevel = GELogLevelOff
	} else {
		currentLogLevel = GELogLevelInfo
	}
}
