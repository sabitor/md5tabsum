package main

import (
	"flag"
	"fmt"
	"md5tabsum/dbms"
	"md5tabsum/log"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	OK = iota
	ERROR

	VERSION    = "1.1.1"
	EXECUTABLE = "md5tabsum"
)

var (
	// Password store file
	passwordStore string
	// Map to store instances and their password
	instancePassword = make(map[string]string)
	// Application return
	rc int
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

// calcMD5TableCheckSum encapsulates the workflow how to compile the MD5 checksum of a database table.
func calcMD5TableCheckSum(instance string) <-chan int {
	out := make(chan int)

	go func() {
		defer close(out)

		// open database connection
		password := instancePassword[instance]
		db, err := instanceName(instance).OpenDB(password)
		if err != nil {
			out <- ERROR
			return
		}
		defer instanceName(instance).CloseDB(db)
		// query database
		err = instanceName(instance).QueryDB(db)
		if err != nil {
			out <- ERROR
			return
		}
		// success
		out <- OK
	}()

	return out
}

// ----------------------------------------------------------------------------
func main() {
	// Parse command line arguments
	cfg, version, passwordstore, instance := parseCmdArgs()

	// Print version to STDOUT
	if *version {
		fmt.Printf("%s %s\n\n", EXECUTABLE, VERSION)
		os.Exit(OK)
	}

	// Read config file
	if err := setupEnv(cfg); err != nil {
		log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, err.Error())
		os.Exit(ERROR)
	}

	if *passwordstore == "" {
		// -- Start workflow --
		log.StartLogService()
		defer log.StopLogService()

		log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[Version]: "+VERSION)
		cfgPath, _ := filepath.Abs(*cfg)
		log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[ConfigFile]: "+cfgPath)
		log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[PasswordStore]: "+passwordStore)

		// Read instance passwords from password store
		if err := readPasswordStore(); err != nil {
			log.WriteLog(log.BASIC, log.BASIC, log.BOTH, err.Error())
			os.Exit(ERROR)
		}

		// Start calculation
		var md5CalcRc []<-chan int
		for k := range instanceToConfig {
			md5CalcRc = append(md5CalcRc, calcMD5TableCheckSum(k))
		}
		// Block until all return codes are available -> calculate overall return code
		for _, v := range md5CalcRc {
			rc |= <-v
		}

		log.WriteLog(log.BASIC, log.BASIC, log.LOGFILE, "[Rc]: "+strconv.Itoa(rc))
		// Wait for the last log entry to be written
		time.Sleep(time.Millisecond * 100)
	} else {
		// -- Password store management --
		if *passwordstore == "create" {
			if err := createInstance(); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "add" {
			if *instance == "" {
				log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "To add an instance and its password in the password store the instance command option '-i <instance name>' is required.")
				os.Exit(ERROR)
			}
			if err := addInstance(instance); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "delete" {
			if *instance == "" {
				log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "To delete an instance and its password from the password store the instance command option '-i <instance name>' is required.")
				os.Exit(ERROR)
			}
			if err := deleteInstance(instance); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "update" {
			if *instance == "" {
				log.WriteLog(log.BASIC, log.BASIC, log.STDOUT, "To update an instance password in the password store the instance command option '-i <instance name>' is required.")
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
