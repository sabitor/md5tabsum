package main

import (
	"flag"
	"fmt"
	"md5tabsum/dbms"
	// "md5tabsum/log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	// "time"

	sLog "github.com/sabitor/simplelog"
)

const (
	OK = iota
	ERROR

	VERSION    = "1.1.1"
	EXECUTABLE = "md5tabsum"
)

var (
	// password store file
	gPasswordStore string
	// map to store instances and their password
	gInstancePassword = make(map[string]string)
)

// instanceName validates the existence of a given DBMS instance name in the instaneToConfig map.
// If the DBMS instance was found, the instance name is returned.
// If the DBMS instance wasn't found, the program will terminate.
func instanceName(instance string) dbms.Database {
	if v, ok := instanceToConfig[instance]; ok {
		return v
	}
	msg := "key '" + instance + "' doesn't exist"
	panic(msg)
}

// parseCmdArgs parses for command line arguments.
// If nothing was specified, corresponding defaults are going to be used.
func parseCmdArgs() (*string, *bool, *string, *string) {
	cfg := flag.String("c", "md5tabsum.cfg", "config file name")
	version := flag.Bool("v", false, "version information")
	password := flag.String("p", "", "password store action\n  create - creates the password store based on the instances stored in the config file\n  add    - adds a specific instance and its password in the password store\n  update - updates the password of a specific instance in the password store\n  delete - deletes a specific instance and its password from the password store\n  show   - shows all stored instances in the password store")
	instance := flag.String("i", "", "instance name\n  The defined format is <DBMS>.<instance ID>")
	flag.Parse()

	return cfg, version, password, instance
}

// compileMD5CheckSum encapsulates the workflow how to compile the MD5 checksum of a database table.
// The steps are: 1) open a database connection, 2) execution of SQL commands, 3) close database connection.
func compileMD5CheckSum(instance string, wg *sync.WaitGroup, rcChan chan<- int) {
	defer wg.Done()

	// open database connection
	password := gInstancePassword[instance]
	db, err := instanceName(instance).OpenDB(password)
	if err != nil {
		rcChan <- ERROR
		return
	}
	// close database connection
	defer instanceName(instance).CloseDB(db)
	// query database
	err = instanceName(instance).QueryDB(db)
	if err != nil {
		rcChan <- ERROR
		return
	}
	// success
	rcChan <- OK
}

// ----------------------------------------------------------------------------
func main() {
	var rc int
	var wg sync.WaitGroup

	// Parse command line arguments
	cfg, version, passwordstore, instance := parseCmdArgs()

	// Print version to STDOUT
	if *version {
		fmt.Printf("%s %s\n\n", EXECUTABLE, VERSION)
		os.Exit(OK)
	}

	// Read config file
	if err := setupEnv(cfg); err != nil {
		// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, err.Error())
		sLog.WriteToStdout("", err.Error())
		os.Exit(ERROR)
	}

	sLog.StartService("md5tabsum.log", 10)
	defer sLog.StopService()

	if *passwordstore == "" {
		// log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[Version]: "+VERSION)
		sLog.WriteToFile("", "[Version]: "+VERSION)
		cfgPath, _ := filepath.Abs(*cfg)
		// log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[ConfigFile]: "+cfgPath)
		sLog.WriteToFile("", "[ConfigFile]: "+cfgPath)
		// log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[PasswordStore]: "+gPasswordStore)
		sLog.WriteToFile("", "[PasswordStore]: "+gPasswordStore)

		// Read instance passwords from password store
		if err := readPasswordStore(); err != nil {
			// log.WriteLog(log.BASIC, log.BASIC, log.BOTH, err.Error())
			sLog.WriteToMultiple("", err.Error())
			os.Exit(ERROR)
		}

		// Compile MD5 table checksum for all configured DBMS instances
		rcChan := make(chan int, len(instanceToConfig))
		for k := range instanceToConfig {
			wg.Add(1)
			go compileMD5CheckSum(k, &wg, rcChan)
		}
		wg.Wait()
		close(rcChan)

		// Calculate return code (rc of all go routines are considered)
		for i := range rcChan {
			rc |= i
		}

		// log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[Rc]: "+strconv.Itoa(rc))
		sLog.WriteToFile("", "[Rc]: "+strconv.Itoa(rc))

		// STILL REQUIRED? CHECK!
		// Wait for the last log entry to be written
		// time.Sleep(time.Millisecond * 100)
	} else {
		// -- Password store management --
		if *passwordstore == "create" {
			if err := createInstance(); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "add" {
			if *instance == "" {
				// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "To add an instance and its password in the password store the instance command option '-i <instance name>' is required.")
				sLog.WriteToStdout("", "To add an instance and its password in the password store the instance command option '-i <instance name>' is required.")
				os.Exit(ERROR)
			}
			if err := addInstance(instance); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "delete" {
			if *instance == "" {
				// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "To delete an instance and its password from the password store the instance command option '-i <instance name>' is required.")
				sLog.WriteToStdout("", "To delete an instance and its password from the password store the instance command option '-i <instance name>' is required.")
				os.Exit(ERROR)
			}
			if err := deleteInstance(instance); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "update" {
			if *instance == "" {
				// log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "To update an instance password in the password store the instance command option '-i <instance name>' is required.")
				sLog.WriteToStdout("", "To update an instance password in the password store the instance command option '-i <instance name>' is required.")
				os.Exit(ERROR)
			}
			if err := updateInstance(instance); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "show" {
			if err := showInstance(); err != nil {
				os.Exit(ERROR)
			}
		}
	}

	os.Exit(rc)
}
