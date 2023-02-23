package log

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/syrinsecurity/gologger"
)

const (
	// Specifies the log target
	LOGFILE = iota
	STDOUT
	BOTH
)

const (
	// Specifies the log detail level
	BASIC = iota
	MEDIUM
	FULL
)

var (
	// mutex to lock/unlock the log
	mtx sync.Mutex
	// log handle
	log *gologger.CustomLogger
)

// logTimestamp returns the current time in a defined format.
func logTimestamp() string {
	ts := time.Now()
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%03d", ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), int(math.Round(float64(ts.Nanosecond()/1000000))))
}

// LogHandler declares a global log handle
func LogHandler(logName string) {
	log = gologger.NewCustomLogger(logName, "", 0)
}

// StartLogService starts the log service.
func StartLogService() {
	go log.Service()
	log.Write("-------------------------------------------------------------------------------")
}

// StopLogService stops the log service.
func StopLogService() {
	log.Close()
}

func WriteLog(msgLogLevel int, configLogLevel int, logTarget int, messages ...string) {
	if configLogLevel >= msgLogLevel {
		mtx.Lock()
		if logTarget != STDOUT {
			log.Write(logTimestamp())
		}
		for _, message := range messages {
			switch logTarget {
			case LOGFILE:
				log.Write(message)
			case STDOUT:
				fmt.Println(message)
			case BOTH:
				log.WritePrint(message)
			}
		}
		mtx.Unlock()
	} else {
		return
	}
}
