package main

import (
	"flag"
	"fmt"
	// "md5tabsum/config"
	// "md5tabsum/dbms"
	"strings"

	// "md5tabsum/log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	// "time"

	sLog "github.com/sabitor/simplelog"
)

// message catalog
const (
	mm000 = "config file name"
	mm001 = "instance name\n  The defined format is <DBMS>.<instance ID>"
	mm002 = "password store command\n  create - creates the password store based on the instances stored in the config file\n  add    - adds a specific instance and its password in the password store\n  update - updates the password of a specific instance in the password store\n  delete - deletes a specific instance and its password from the password store\n  show   - shows all stored instances in the password store"
	mm003 = "version information"
	mm004 = "to add instance credentials in the password store the command option '-i <instance name>' is required"
	mm005 = "to delete instance credentials from the password store the command option '-i <instance name>' is required"
	mm006 = "to update instance credentials in the password store the command option '-i <instance name>' is required"
	mm007 = "the specified instance doesn't exist in the password store"
	mm008 = "the specified instance already exists in the password store"
	mm009 = "something went wrong while determining the nonce size"
	mm010 = "unsupported password store command specified"
	mm011 = "unsupported log level parameter specified - supported parameters are INFO, DEBUG and TRACE"
	mm012 = "this branch shouldn't be reached"
	mm013 = "the Logfile parameter isn't configured"
	mm014 = "the Passwordstore parameter isn't configured"
)

const (
	md5Ok = iota
	md5Error
)

const (
	programVersion string = "1.2.1"
	executableName string = "md5tabsum"
	configName     string = "md5tabsum.cfg"
)

const (
	// specifies the log level
	INFO  = iota // the standard log level
	DEBUG        // less granular compared to the TRACE level
	TRACE        // the most fine-grained information
)

// parseCmdArgs parses for command line arguments.
// If nothing was specified, defined defaults are used.
func parseCmdArgs() (*string, *string, *string, *bool) {
	cfg := flag.String("c", configName, mm000)
	instance := flag.String("i", "", mm001)
	password := flag.String("p", "", mm002)
	version := flag.Bool("v", false, mm003)
	flag.Parse()

	return cfg, instance, password, version
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
	sLog.Startup(100)
	defer sLog.Shutdown(false)
	sLog.SetPrefix(sLog.FILE, "#2006-01-02 15:04:05.000000#")

	// parse command line arguments
	cfg, instance, passwordStore, version := parseCmdArgs()
	if *version {
		fmt.Printf("%s %s\n", executableName, programVersion)
		return md5Ok
	}

	// read config file
	if err := setupEnv(cfg); err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, err.Error())
		sLog.Write(sLog.STDOUT, err.Error())
		return md5Error
	}

	*passwordStore = strings.ToLower(*passwordStore)
	if *passwordStore != "" {
		if *passwordStore == "create" {
			if err := createInstance(); err != nil {
				// sLog.Write(sLog.STDOUT, err)
				return md5Error
			}
		} else if *passwordStore == "add" {
			if *instance == "" {
				// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "To add an instance and its password in the password store the instance command option '-i <instance name>' is required.")
				sLog.Write(sLog.STDOUT, mm004)
				return md5Error
			}
			if err := addInstance(instance); err != nil {
				sLog.Write(sLog.STDOUT, err)
				return md5Error
			}
		} else if *passwordStore == "delete" {
			if *instance == "" {
				// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "To delete an instance and its password from the password store the instance command option '-i <instance name>' is required.")
				sLog.Write(sLog.STDOUT, mm005)
				return md5Error
			}
			if err := deleteInstance(instance); err != nil {
				sLog.Write(sLog.STDOUT, err)
				return md5Error
			}
		} else if *passwordStore == "update" {
			if *instance == "" {
				// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "To update an instance password in the password store the instance command option '-i <instance name>' is required.")
				sLog.Write(sLog.STDOUT, mm006)
				return md5Error
			}
			if err := updateInstance(instance); err != nil {
				sLog.Write(sLog.STDOUT, err)
				return md5Error
			}
		} else if *passwordStore == "show" {
			if err := showInstance(); err != nil {
				sLog.Write(sLog.STDOUT, err)
				return md5Error
			}
		} else {
			// unsupported command found
			sLog.Write(sLog.STDOUT, mm010)
			return md5Error
		}
	} else {
		var wg sync.WaitGroup

		// log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[Version]: "+VERSION)
		sLog.Write(sLog.FILE, "Version:", programVersion)
		cfgPath, _ := filepath.Abs(*cfg)
		// log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[ConfigFile]: "+cfgPath)
		sLog.Write(sLog.FILE, "ConfigFile:", cfgPath)
		// log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[PasswordStore]: "+gPasswordStore)
		sLog.Write(sLog.FILE, "PasswordStore:", passwordStoreFile)

		// Read instance passwords from password store
		if err := readPasswordStore(); err != nil {
			// log.WriteLog(log.BASIC, log.BASIC, log.BOTH, err.Error())
			sLog.Write(sLog.MULTI, err.Error())
			return md5Error
		}

		// compile MD5 table checksum for all configured DBMS instances
		results := make(chan int, len(instanceToConfig))
		for k := range instanceToConfig {
			wg.Add(1)
			go compileMD5TableSum(k, &wg, results)
		}
		wg.Wait()
		close(results)

		// calculate return code (rc of all go routines are considered)
		for i := range results {
			rc |= i
		}

		// log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[Rc]: "+strconv.Itoa(rc))
		sLog.Write(sLog.FILE, "Return Code: "+strconv.Itoa(rc))
	}

	return rc
}

// main starts the application workflow and returns the return code to the caller
func main() {
	os.Exit(run())
}
