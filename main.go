package main

import (
	"flag"
	"fmt"
	"md5tabsum/constant"
	"md5tabsum/dbms"
	"os"
	"path/filepath"
	"sync"
)

var (
	// password store file
	gPasswordStore string
	// map to store instances and their password
	gInstancePassword = make(map[string]string)
)

// Instance validates the existence of a given DBMS instance in the instance configuration.
// If the DBMS instance was found, the instance name is returned.
// If the DBMS instance wasn't found, the program will terminate.
func instance(dbmsInstance string) dbms.Database {
	if v, ok := gDbms[dbmsInstance]; ok {
		return v
	}
	msg := "key '" + dbmsInstance + "' doesn't exist"
	panic(msg)
}

// parseCmdArgs parses for command line arguments.
// If nothing was specified, corresponding defaults are going to be used.
func parseCmdArgs() (*string, *bool, *string, *string) {
	cfg := flag.String("c", "md5tabsum.cfg", "config file name")
	version := flag.Bool("v", false, "version information")
	password := flag.String("p", constant.EMPTYSTRING, "password store action\n  create - creates the password store based on the instances stored in the config file\n  add    - adds a specific instance and its password in the password store\n  update - updates the password of a specific instance in the password store\n  delete - deletes a specific instance and its password from the password store\n  show   - shows all stored instances in the password store")
	instance := flag.String("i", constant.EMPTYSTRING, "instance name\n  The defined format is <DBMS>.<instance ID>")
	flag.Parse()

	return cfg, version, password, instance
}

// compileMD5CheckSum encapsulates the workflow how to compile the MD5 checksum of a database table.
// The steps are: 1) open a database connection, 2) execution of SQL commands, 3) close database connection.
func compileMD5CheckSum(dbmsInstance string, wg *sync.WaitGroup, c chan<- int) {
	defer wg.Done()

	// open database connection

	password := gInstancePassword[dbmsInstance]
	db, err := instance(dbmsInstance).OpenDB(password)
	if err != nil {
		c <- constant.ERROR
		return
	}
	// close database connection
	defer instance(dbmsInstance).CloseDB(db)
	// query database
	err = instance(dbmsInstance).QueryDB(db)
	if err != nil {
		c <- constant.ERROR
		return
	}
	// success
	c <- constant.OK
}

// ----------------------------------------------------------------------------
func main() {
	var rc int
	var wg sync.WaitGroup

	// parse command line arguments
	cfg, version, passwordstore, instance := parseCmdArgs()

	// print version to STDOUT
	if *version {
		fmt.Printf("%s %s\n\n", constant.EXECUTABLE, constant.VERSION)
		os.Exit(constant.OK)
	}

	// read config file
	if err := setupEnv(cfg); err != nil {
		writeLogBasic(constant.STDOUT, err.Error())
		os.Exit(constant.ERROR)
	}

	if *passwordstore == constant.EMPTYSTRING {
		// -- start workflow --
		startLogService()
		defer stopLogService()
		cfgPath, _ := filepath.Abs(*cfg)
		message := fmt.Sprintf("%s [config file: %s]", getLogTimestamp(), cfgPath)
		writeLogBasic(constant.LOGFILE, message)
		message = fmt.Sprintf("%s [password store: %s]", getLogTimestamp(), gPasswordStore)
		writeLogBasic(constant.LOGFILE, message)

		// read instance passwords from password store
		if err := readPasswordStore(); err != nil {
			writeLogBasic(constant.BOTH, err.Error())
			os.Exit(constant.ERROR)
		}

		// compile MD5 table checksum for all configured DBMS instances
		c := make(chan int, len(gDbms))
		for k := range gDbms {
			wg.Add(1)
			go compileMD5CheckSum(k, &wg, c)
		}
		wg.Wait()
		close(c)

		// calculate return code (rc of all go routines are considered)
		for i := range c {
			rc |= i
		}

		message = fmt.Sprintf("%s [rc=%d]", getLogTimestamp(), rc)
		writeLogBasic(constant.LOGFILE, message)
	} else {
		// -- password store management --
		if *passwordstore == "create" {
			if err := createInstance(); err != nil {
				os.Exit(constant.ERROR)
			}
		} else if *passwordstore == "add" {
			if *instance == constant.EMPTYSTRING {
				writeLogBasic(constant.STDOUT, "To add an instance and its password in the password store the instance command option '-i <instance name>' is required.")
				os.Exit(constant.ERROR)
			}
			if err := addInstance(instance); err != nil {
				os.Exit(constant.ERROR)
			}
		} else if *passwordstore == "delete" {
			if *instance == constant.EMPTYSTRING {
				writeLogBasic(constant.STDOUT, "To delete an instance and its password from the password store the instance command option '-i <instance name>' is required.")
				os.Exit(constant.ERROR)
			}
			if err := deleteInstance(instance); err != nil {
				os.Exit(constant.ERROR)
			}
		} else if *passwordstore == "update" {
			if *instance == constant.EMPTYSTRING {
				writeLogBasic(constant.STDOUT, "To update an instance password in the password store the instance command option '-i <instance name>' is required.")
				os.Exit(constant.ERROR)
			}
			if err := updateInstance(instance); err != nil {
				os.Exit(constant.ERROR)
			}
		} else if *passwordstore == "show" {
			if err := showInstance(); err != nil {
				os.Exit(constant.ERROR)
			}
		}
	}

	os.Exit(rc)
}
