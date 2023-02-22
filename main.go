package main

import (
	"flag"
	"fmt"
	"md5tabsum/constant"
	"md5tabsum/dbms"
	"md5tabsum/log"
	"os"
	"path/filepath"
	"sync"
	"time"
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
	password := flag.String("p", constant.EMPTYSTRING, "password store action\n  create - creates the password store based on the instances stored in the config file\n  add    - adds a specific instance and its password in the password store\n  update - updates the password of a specific instance in the password store\n  delete - deletes a specific instance and its password from the password store\n  show   - shows all stored instances in the password store")
	instance := flag.String("i", constant.EMPTYSTRING, "instance name\n  The defined format is <DBMS>.<instance ID>")
	flag.Parse()

	return cfg, version, password, instance
}

// compileMD5CheckSum encapsulates the workflow how to compile the MD5 checksum of a database table.
// The steps are: 1) open a database connection, 2) execution of SQL commands, 3) close database connection.
func compileMD5CheckSum(instance string, wg *sync.WaitGroup, c chan<- int) {
	defer wg.Done()

	// open database connection

	password := gInstancePassword[instance]
	db, err := instanceName(instance).OpenDB(password)
	if err != nil {
		c <- constant.ERROR
		return
	}
	// close database connection
	defer instanceName(instance).CloseDB(db)
	// query database
	err = instanceName(instance).QueryDB(db)
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
		log.WriteLog(0, constant.STDOUT, "hallo", err.Error())
		// log.WriteLogBasic(constant.STDOUT, err.Error())
		os.Exit(constant.ERROR)
	}

	if *passwordstore == constant.EMPTYSTRING {
		// -- start workflow --
		log.StartLogService()
		defer log.StopLogService()

		log.WriteLog2(0, 0, constant.LOGFILE, "Version: "+constant.VERSION)
		cfgPath, _ := filepath.Abs(*cfg)
		var message string
		log.WriteLog2(0, 0, constant.LOGFILE, "Config file: "+cfgPath)
		log.WriteLog2(0, 0, constant.LOGFILE, "Password store: "+gPasswordStore)

		// read instance passwords from password store
		if err := readPasswordStore(); err != nil {
			// log.WriteLogBasic(constant.BOTH, err.Error())
			log.WriteLog(0, constant.BOTH, constant.EMPTYSTRING, err.Error())
			os.Exit(constant.ERROR)
		}

		// compile MD5 table checksum for all configured DBMS instances
		c := make(chan int, len(instanceToConfig))
		for k := range instanceToConfig {
			wg.Add(1)
			go compileMD5CheckSum(k, &wg, c)
		}
		wg.Wait()
		close(c)

		// calculate return code (rc of all go routines are considered)
		for i := range c {
			rc |= i
		}

		message = fmt.Sprintf("%s [rc=%d]", log.LogTimestamp(), rc)
		// log.WriteLogBasic(constant.LOGFILE, message)
		log.WriteLog(0, constant.LOGFILE, constant.EMPTYSTRING, message)
		// wait for the log to be written
		time.Sleep(time.Millisecond * 100)
	} else {
		// -- password store management --
		if *passwordstore == "create" {
			if err := createInstance(); err != nil {
				os.Exit(constant.ERROR)
			}
		} else if *passwordstore == "add" {
			if *instance == constant.EMPTYSTRING {
				// log.WriteLogBasic(constant.STDOUT, "To add an instance and its password in the password store the instance command option '-i <instance name>' is required.")
				log.WriteLog(0, constant.STDOUT, constant.EMPTYSTRING, "To add an instance and its password in the password store the instance command option '-i <instance name>' is required.")
				os.Exit(constant.ERROR)
			}
			if err := addInstance(instance); err != nil {
				os.Exit(constant.ERROR)
			}
		} else if *passwordstore == "delete" {
			if *instance == constant.EMPTYSTRING {
				// log.WriteLogBasic(constant.STDOUT, "To delete an instance and its password from the password store the instance command option '-i <instance name>' is required.")
				log.WriteLog(0, constant.STDOUT, constant.EMPTYSTRING, "To delete an instance and its password from the password store the instance command option '-i <instance name>' is required.")
				os.Exit(constant.ERROR)
			}
			if err := deleteInstance(instance); err != nil {
				os.Exit(constant.ERROR)
			}
		} else if *passwordstore == "update" {
			if *instance == constant.EMPTYSTRING {
				// log.WriteLogBasic(constant.STDOUT, "To update an instance password in the password store the instance command option '-i <instance name>' is required.")
				log.WriteLog(0, constant.STDOUT, constant.EMPTYSTRING, "To update an instance password in the password store the instance command option '-i <instance name>' is required.")
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
