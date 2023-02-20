package log

import (
	"fmt"
	"math"
	"md5tabsum/constant"
	"strings"
	"sync"
	"time"

	"github.com/syrinsecurity/gologger"
)

var (
	// global mutex to lock/unlock the log
	gMtx sync.Mutex
	// log handle
	log *gologger.CustomLogger
)

// setupLogHandler declares a global log handle
func LogHandler(logName string) {
	log = gologger.NewCustomLogger(logName, constant.EMPTYSTRING, 0)
}

// Instance extracts the instance part from the objId.
func inst(objId string) string {
	element := strings.Split(objId, ".")
	return fmt.Sprintf("%s.%s", element[0], element[1])
}

// getLogTimestamp returns the current time in a defined format.
func getLogTimestamp() string {
	ts := time.Now()
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%03d", ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), int(math.Round(float64(ts.Nanosecond()/1000000))))
}

// startLogService starts the log service.
func startLogService() {
	go log.Service()
	log.Write("-------------------------------------------------------------------------------")
	header := fmt.Sprintf("[%s: version %s]", constant.EXECUTABLE, constant.VERSION)
	log.Write(getLogTimestamp(), header)
}

// stopLogService stops the log service.
func stopLogService() {
	log.Close()
}

// buildLogMessage builds a log message by concatenating strings.
func buildLogMessage(logMessage *string, data *string) {
	if *logMessage != constant.EMPTYSTRING {
		*logMessage += ", "
	}
	*logMessage += *data
}

// writeLogBasic writes messages one to one either into a logfile, to STDOUT or both.
func writeLogBasic(logTarget int, message string) {
	switch logTarget {
	case constant.LOGFILE:
		gMtx.Lock()
		log.Write(message)
		gMtx.Unlock()
	case constant.STDOUT:
		fmt.Println(message)
	case constant.BOTH:
		gMtx.Lock()
		log.Write(message)
		gMtx.Unlock()
		fmt.Println(message)
	}
	// wait for the log to be written
	time.Sleep(time.Millisecond * 100)
}

// writeLog writes messages enriched by a timestamp and defined meta data into a logfile.
func writeLog(msgLogLevel int, objId *string, messages ...string) {
	if gInstanceLogLevel[inst(*objId)] >= msgLogLevel {
		var sectionPrefix string
		if strings.Count(*objId, ".") == 1 {
			sectionPrefix = "instance"
		} else {
			sectionPrefix = "object"
		}

		gMtx.Lock()
		header := fmt.Sprintf("%s [%s: %s]", getLogTimestamp(), sectionPrefix, *objId)
		log.Write(header)
		for _, message := range messages {
			log.Write(message)
		}
		gMtx.Unlock()
	}
}
