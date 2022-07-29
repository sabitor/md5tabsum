package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// parseCmdArgs parses for command line arguments.
// If nothing was specified, corresponding defaults are going to be used.
func parseCmdArgs() (*string, *bool, *string, *string) {
	cfg := flag.String("c", "md5tabsum.cfg", "config file name")
	version := flag.Bool("v", false, "version information")
	password := flag.String("p", EMPTYSTRING, "password store action\n  create - creates the password store based on the instances stored in the config file\n  add    - adds a specific instance and its password in the password store\n  update - updates the password of a specific instance in the password store\n  delete - deletes a specific instance and its password from the password store\n  show   - shows all stored instances in the password store")
	instance := flag.String("i", EMPTYSTRING, "instance name\n  The defined format is <DBMS>.<instance ID>")
	flag.Parse()

	return cfg, version, password, instance
}

// compileMD5CheckSum encapsulates the workflow a go routine is going to process after it has been started.
// The steps are: 1) open a database connection, 2) execution of SQL commands, 3) close database connection.
func compileMD5CheckSum(dbmsInstance string, wg *sync.WaitGroup, c chan<- int) {
	defer wg.Done()

	// open database connection
	db, err := Dbms(dbmsInstance).openDB()
	if err != nil {
		c <- ERROR
		return
	}
	// close database connection
	defer Dbms(dbmsInstance).closeDB(db)
	// query database
	err = Dbms(dbmsInstance).queryDB(db)
	if err != nil {
		c <- ERROR
		return
	}
	// success
	c <- OK
}

// ----------------------------------------------------------------------------
func main() {
	var rc int
	var wg sync.WaitGroup

	// parse command line arguments
	cfg, version, passwordstore, instance := parseCmdArgs()

	// print version to STDOUT
	if *version {
		fmt.Printf("%s %s\n\n", EXECUTABLE, VERSION)
		os.Exit(OK)
	}

	// read config file
	if err := setupEnv(cfg); err != nil {
		writeLogBasic(STDOUT, err.Error())
		os.Exit(ERROR)
	}

	if *passwordstore == EMPTYSTRING {
		// -- start workflow --
		startLogService()
		defer stopLogService()
		cfgPath, _ := filepath.Abs(*cfg)
		message := fmt.Sprintf("%s [config file: %s]", getLogTimestamp(), cfgPath)
		writeLogBasic(LOGFILE, message)
		message = fmt.Sprintf("%s [password store: %s]", getLogTimestamp(), gPasswordStore)
		writeLogBasic(LOGFILE, message)

		// read instance passwords from password store
		if err := readPasswordStore(); err != nil {
			writeLogBasic(BOTH, err.Error())
			os.Exit(ERROR)
		}

		// compile table checksum for all configured DBMS instances
		c := make(chan int, len(gDbms))
		for k := range gDbms {
			wg.Add(1)
			go compileMD5CheckSum(k, &wg, c)
		}
		wg.Wait()
		close(c)

		// calculate overall return code (rc of all go routines are considered)
		for i := range c {
			rc |= i
		}

		message = fmt.Sprintf("%s [rc=%d]", getLogTimestamp(), rc)
		writeLogBasic(LOGFILE, message)
	} else {
		// -- password store management --
		if *passwordstore == "create" {
			if err := createInstance(); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "add" {
			if *instance == EMPTYSTRING {
				writeLogBasic(STDOUT, "To add an instance and its password in the password store the instance command option '-i <instance name>' is required.")
				os.Exit(ERROR)
			}
			if err := addInstance(instance); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "delete" {
			if *instance == EMPTYSTRING {
				writeLogBasic(STDOUT, "To delete an instance and its password from the password store the instance command option '-i <instance name>' is required.")
				os.Exit(ERROR)
			}
			if err := deleteInstance(instance); err != nil {
				os.Exit(ERROR)
			}
		} else if *passwordstore == "update" {
			if *instance == EMPTYSTRING {
				writeLogBasic(STDOUT, "To update an instance password in the password store the instance command option '-i <instance name>' is required.")
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
