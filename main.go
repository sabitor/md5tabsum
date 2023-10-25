package main

import (
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/sabitor/simplelog"
)

// message catalog
const (
	mm000 string = "config file name"
	mm001 string = "instance name\n  The defined format is <predefined DBMS name>.<instance ID>\n  Predefined DBMS names are: exasol, mysql, mssql, oracle, postgresql"
	mm002 string = "password store command\n  init   - initializes the password store based on the DBMS instances setup in the config file\n  add    - adds a passed DBMS instance and its password to the password store\n  update - updates the password of the passed DBMS instance in the password store\n  delete - deletes the passed DBMS instance record from the password store\n  show   - shows all DBMS instances records saved in the password store"
	mm003 string = "log detail level: DEBUG (extended logging), TRACE (full logging)"
	mm004 string = "to add instance credentials in the password store the command option '-i <instance name>' is required"
	mm005 string = "to delete instance credentials from the password store the command option '-i <instance name>' is required"
	mm006 string = "to update instance credentials in the password store the command option '-i <instance name>' is required"
	mm007 string = "the specified instance does not exist in the password store"
	mm008 string = "the specified instance already exists in the password store"
	mm009 string = "something went wrong while determining the nonce size"
	mm010 string = "unsupported password store command specified"
	mm012 string = "this branch should not be reached"
	mm013 string = "the Logfile parameter is not configured"
	mm014 string = "the Passwordstore parameter is not configured"
	mm015 string = "the Passwordstorekey parameter is not configured"
	mm016 string = "the password store specified by the Passwordstore parmeter does not exist"
	mm017 string = "DBMS instance section '%1' does not contain an instance ID"
	mm018 string = "remove instance %1 from the password store"
)

const (
	md5Ok = iota
	md5Error
)

const (
	programVersion    string = "1.2.1"
	executableName    string = "md5tabsum"
	defaultConfigName string = "md5tabsum.cfg"
)

const (
	// specifies the log level
	info  = iota // the standard log level
	debug        // less granular compared to the TRACE level
	trace        // the most fine-grained information
)

// collection of command line parameters
type parameter struct {
	cfg           string
	instance      string
	passwordStore string
	logLevel      int
}

// command line parameter
var pr parameter

// parseParameter parses for command line parameters.
func parseParameter() {
	flag.StringVar(&pr.cfg, "c", defaultConfigName, mm000)
	flag.StringVar(&pr.instance, "i", "", mm001)
	flag.StringVar(&pr.passwordStore, "p", "", mm002)
	loglevelStr := ""
	flag.StringVar(&loglevelStr, "l", "", mm003)
	flag.Parse()

	// convert provided log level into integer
	switch strings.ToUpper(loglevelStr) {
	case "DEBUG":
		pr.logLevel = debug
	case "TRACE":
		pr.logLevel = trace
	default:
		pr.logLevel = info
	}
}

// compileMD5TableSum encapsulates the workflow how to compile the MD5 checksum of a database table.
func compileMD5TableSum(instance string, wg *sync.WaitGroup, result chan<- int) {
	defer wg.Done()

	// open database connection
	password := instancePassword[instance]
	db, err := instanceName(instance).openDB(password)
	if err != nil {
		result <- md5Error
		return
	}
	// close database connection
	defer instanceName(instance).closeDB(db)
	// query database
	err = instanceName(instance).queryDB(db)
	if err != nil {
		result <- md5Error
		return
	}
	// success
	result <- md5Ok
}

// run is the entry point of the application logic.
func run() int {
	var rc int

	// init log
	simplelog.Startup(100)
	defer simplelog.Shutdown(false)
	simplelog.SetPrefix(simplelog.FILE, "#2006-01-02 15:04:05.000000#")

	// parse command line parameter
	parseParameter()

	// read config file
	if err := setupEnv(pr.cfg); err != nil {
		simplelog.Write(simplelog.STDOUT, err.Error())
		return md5Error
	}

	programName, _ := os.Executable()
	simplelog.Write(simplelog.FILE, programName, "version:", programVersion)
	cfgPath, _ := filepath.Abs(pr.cfg)
	simplelog.Write(simplelog.FILE, "Configfile:", cfgPath)
	simplelog.Write(simplelog.FILE, "Passwordstore:", passwordStoreFile)
	simplelog.Write(simplelog.FILE, "Passwordstorekey:", passwordStoreKeyFile)

	// check for password store command
	if pr.passwordStore != "" {
		pr.passwordStore = strings.ToLower(pr.passwordStore)
		simplelog.Write(simplelog.FILE, "Passwordstore command:", pr.passwordStore)
		if pr.passwordStore == "init" {
			if err := initPWS(); err != nil {
				simplelog.Write(simplelog.MULTI, err.Error())
				rc = md5Error
			}
		} else {
			// password store must have been already initialized; read instance password(s) from it
			if err := readPasswordStore(); err != nil {
				simplelog.Write(simplelog.MULTI, err.Error())
				rc = md5Error
			} else {
				if pr.passwordStore == "add" {
					if pr.instance == "" {
						simplelog.Write(simplelog.MULTI, mm004)
						rc = md5Error
					} else {
						if err := addInstance(pr.instance); err != nil {
							simplelog.Write(simplelog.MULTI, err.Error())
							rc = md5Error
						}
					}
				} else if pr.passwordStore == "delete" {
					if pr.instance == "" {
						simplelog.Write(simplelog.MULTI, mm005)
						rc = md5Error
					} else {
						if err := deleteInstance(pr.instance); err != nil {
							simplelog.Write(simplelog.MULTI, err.Error())
							rc = md5Error
						}
					}
				} else if pr.passwordStore == "update" {
					if pr.instance == "" {
						simplelog.Write(simplelog.MULTI, mm006)
						rc = md5Error
					} else {
						if err := updateInstance(pr.instance); err != nil {
							simplelog.Write(simplelog.MULTI, err.Error())
							rc = md5Error
						}
					}
				} else if pr.passwordStore == "show" {
					showInstance()
				} else if pr.passwordStore == "sync" {
					syncPWS()
				} else {
					// unsupported password store command specified
					simplelog.Write(simplelog.MULTI, mm010)
					rc = md5Error
				}
			}
		}
	} else {
		// read instance password(s) from password store
		if err := readPasswordStore(); err != nil {
			simplelog.Write(simplelog.MULTI, err.Error())
			rc = md5Error
		} else {
			var wg sync.WaitGroup
			// compile MD5 table checksum for all active DBMS instances
			rcGoRoutines := make(chan int, len(instanceActive))
			for k := range instanceActive {
				wg.Add(1)
				go compileMD5TableSum(k, &wg, rcGoRoutines)
			}
			wg.Wait()
			close(rcGoRoutines)

			// calculate overall return code
			for rcSingle := range rcGoRoutines {
				rc |= rcSingle
			}
		}
	}

	simplelog.Write(simplelog.FILE, "Return Code: "+strconv.Itoa(rc))
	return rc
}

// main starts the application workflow and returns the return code to the caller
func main() {
	os.Exit(run())
}
