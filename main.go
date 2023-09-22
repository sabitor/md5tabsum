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
	mm000 = "config file name"
	mm001 = "instance name\n  The defined format is <predefined DBMS name>.<instance ID>"
	mm002 = "password store command\n  create - creates the password store based on the instances stored in the config file\n  add    - adds a specific instance and its password in the password store\n  update - updates the password of a specific instance in the password store\n  delete - deletes a specific instance and its password from the password store\n  show   - shows all stored instances in the password store"
	mm003 = "log detail level: DEBUG (extended logging), TRACE (full logging)"
	mm004 = "to add instance credentials in the password store the command option '-i <instance name>' is required"
	mm005 = "to delete instance credentials from the password store the command option '-i <instance name>' is required"
	mm006 = "to update instance credentials in the password store the command option '-i <instance name>' is required"
	mm007 = "the specified instance doesn't exist in the password store"
	mm008 = "the specified instance already exists in the password store"
	mm009 = "something went wrong while determining the nonce size"
	mm010 = "unsupported password store command specified"
	mm012 = "this branch shouldn't be reached"
	mm013 = "the Logfile parameter isn't configured"
	mm014 = "the Passwordstore parameter isn't configured"
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
	var rcOverall int

	// init log
	simplelog.Startup(100)
	defer simplelog.Shutdown(false)
	simplelog.SetPrefix(simplelog.FILE, "#2006-01-02 15:04:05.000000#")

	// parse command line parameter
	parseParameter()

	// read config file
	if err := setupEnv(&pr.cfg); err != nil {
		simplelog.Write(simplelog.STDOUT, err.Error())
		return md5Error
	}

	if pr.passwordStore != "" {
		pr.passwordStore = strings.ToLower(pr.passwordStore)
		if pr.passwordStore == "create" {
			if err := createInstance(); err != nil {
				simplelog.Write(simplelog.STDOUT, err)
				return md5Error
			}
		} else if pr.passwordStore == "add" {
			if pr.instance == "" {
				simplelog.Write(simplelog.STDOUT, mm004)
				return md5Error
			}
			if err := addInstance(pr.instance); err != nil {
				simplelog.Write(simplelog.STDOUT, err)
				return md5Error
			}
		} else if pr.passwordStore == "delete" {
			if pr.instance == "" {
				simplelog.Write(simplelog.STDOUT, mm005)
				return md5Error
			}
			if err := deleteInstance(pr.instance); err != nil {
				simplelog.Write(simplelog.STDOUT, err)
				return md5Error
			}
		} else if pr.passwordStore == "update" {
			if pr.instance == "" {
				simplelog.Write(simplelog.STDOUT, mm006)
				return md5Error
			}
			if err := updateInstance(pr.instance); err != nil {
				simplelog.Write(simplelog.STDOUT, err)
				return md5Error
			}
		} else if pr.passwordStore == "show" {
			if err := showInstance(); err != nil {
				simplelog.Write(simplelog.STDOUT, err)
				return md5Error
			}
		} else {
			// unsupported command found
			simplelog.Write(simplelog.STDOUT, mm010)
			return md5Error
		}
	} else {
		programName, err := os.Executable()
		if err != nil {
			simplelog.Write(simplelog.MULTI, err.Error())
			return md5Error
		}
		simplelog.Write(simplelog.FILE, programName, "version:", programVersion)
		cfgPath, _ := filepath.Abs(pr.cfg)
		simplelog.Write(simplelog.FILE, "ConfigFile:", cfgPath)
		simplelog.Write(simplelog.FILE, "PasswordStore:", passwordStoreFile)

		// read instance password(s) from password store
		if err := readPasswordStore(); err != nil {
			simplelog.Write(simplelog.MULTI, err.Error())
			return md5Error
		}

		var wg sync.WaitGroup
		// compile MD5 table checksum for all configured DBMS instances
		rcGoRoutines := make(chan int, len(instanceToConfig))
		for k := range instanceToConfig {
			wg.Add(1)
			go compileMD5TableSum(k, &wg, rcGoRoutines)
		}
		wg.Wait()
		close(rcGoRoutines)

		// calculate overall return code
		for rc := range rcGoRoutines {
			rcOverall |= rc
		}
		simplelog.Write(simplelog.FILE, "Return Code: "+strconv.Itoa(rcOverall))
	}

	return rcOverall
}

// main starts the application workflow and returns the return code to the caller
func main() {
	os.Exit(run())
}
