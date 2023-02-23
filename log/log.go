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
	// mutex to lock/unlock the log
	mtx sync.Mutex
	// log handle
	log *gologger.CustomLogger
	// map to store instances and their assigned log level
	InstanceToLogLevel = make(map[string]int)
)

// LogHandler declares a global log handle
func LogHandler(logName string) {
	log = gologger.NewCustomLogger(logName, constant.EMPTYSTRING, 0)
}

// Instance extracts the instance part from the objId.
func inst(objId string) string {
	element := strings.Split(objId, ".")
	return fmt.Sprintf("%s.%s", element[0], element[1])
}

// LogTimestamp returns the current time in a defined format.
func LogTimestamp() string {
	ts := time.Now()
	return fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%03d", ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), int(math.Round(float64(ts.Nanosecond()/1000000))))
}

// StartLogService starts the log service.
func StartLogService() {
	go log.Service()
	log.Write("-------------------------------------------------------------------------------")
	// header := fmt.Sprintf("[%s: version %s]", constant.EXECUTABLE, constant.VERSION)
	// log.Write(LogTimestamp(), header)
}

// StopLogService stops the log service.
func StopLogService() {
	log.Close()
}

// BuildLogMessage builds a log message by concatenating strings.
func BuildLogMessage(logMessage *string, data *string) {
	if *logMessage != constant.EMPTYSTRING {
		*logMessage += ", "
	}
	*logMessage += *data
}

// WriteLogBasic writes messages one to one either into a logfile, to STDOUT or both.
// func WriteLogBasic(logTarget int, message string) {
// 	switch logTarget {
// 	case constant.LOGFILE:
// 		mtx.Lock()
// 		log.Write(message)
// 		mtx.Unlock()
// 	case constant.STDOUT:
// 		fmt.Println(message)
// 	case constant.BOTH:
// 		mtx.Lock()
// 		log.Write(message)
// 		mtx.Unlock()
// 		fmt.Println(message)
// 	}
// 	// wait for the log to be written
// 	time.Sleep(time.Millisecond * 100)
// }

// WriteLog writes messages enriched by a timestamp and defined meta data into a logfile.
// func WriteLog(msgLogLevel int, logTarget int, objId string, messages ...string) {
// 	if objId == constant.EMPTYSTRING {
// 		for _, message := range messages {
// 			switch logTarget {
// 			case constant.LOGFILE:
// 				log.Write(message)
// 			case constant.STDOUT:
// 				fmt.Println(message)
// 			case constant.BOTH:
// 				log.Write(message)
// 				fmt.Println(message)
// 			}
// 		}
// 	} else if InstanceToLogLevel[inst(objId)] >= msgLogLevel {
// 		var sectionPrefix string
// 		if strings.Count(objId, ".") == 1 {
// 			sectionPrefix = "instance"
// 		} else {
// 			sectionPrefix = "object"
// 		}

// 		mtx.Lock()
// 		header := fmt.Sprintf("%s [%s: %s]", LogTimestamp(), sectionPrefix, objId)
// 		log.Write(header)
// 		for _, message := range messages {
// 			switch logTarget {
// 			case constant.LOGFILE:
// 				log.Write(message)
// 			case constant.BOTH:
// 				log.Write(message)
// 				fmt.Println(message)
// 			}
// 		}
// 		mtx.Unlock()
// 	} else {
// 		return
// 	}
// }

func WriteLog(msgLogLevel int, logTarget int, objId string, messages ...string) {
	if objId == constant.EMPTYSTRING {
		for _, message := range messages {
			switch logTarget {
			case constant.LOGFILE:
				log.Write(message)
			case constant.STDOUT:
				fmt.Println(message)
			case constant.BOTH:
				log.Write(message)
				fmt.Println(message)
			}
		}
	} else if InstanceToLogLevel[inst(objId)] >= msgLogLevel {
		mtx.Lock()
		log.Write(LogTimestamp())
		for _, message := range messages {
			switch logTarget {
			case constant.LOGFILE:
				log.Write(message)
			case constant.BOTH:
				log.Write(message)
				fmt.Println(message)
			}
		}
		mtx.Unlock()
	} else {
		return
	}
}

func WriteLog2(msgLogLevel int, instLogLevel int, logTarget int, messages ...string) {
	if instLogLevel >= msgLogLevel {
		mtx.Lock()
		log.Write(LogTimestamp())
		for _, message := range messages {
			switch logTarget {
			case constant.LOGFILE:
				log.Write(message)
			case constant.STDOUT:
				fmt.Println(message)
			case constant.BOTH:
				log.Write(message)
				fmt.Println(message)
			}
		}
		mtx.Unlock()
	} else {
		return
	}
}
