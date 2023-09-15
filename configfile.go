package main

import (
	"errors"
	"md5tabsum/dbms"
	//"md5tabsum/log"
	// "os"
	"path/filepath"
	"strconv"
	"strings"

	sLog "github.com/sabitor/simplelog"
	"github.com/spf13/viper"
)

var (
	// Map to store active instances and their assigned configuration
	// mDbms[Key -> DBMS instance name : Value -> DBMS instance config]
	// Example: mDbms["exasol.instance1":exasolDB DBMS interface]
	instanceToConfig = make(map[string]dbms.Database)
	// List of all supported DBMS
	supportedDbms = []string{"exasol", "mysql", "mssql", "oracle", "postgresql"}
)

// setInstanceConfig sets the instance parameters according the parsed config file section
func setInstanceConfig(instance string, v *viper.Viper) {
	var logLvl int
	switch strings.ToUpper(v.GetString("loglevel")) {
	case "BASIC":
		logLvl = 0
	case "MEDIUM":
		logLvl = 1
	case "FULL":
		logLvl = 2
	default:
		panic("An unsupported log detail level has been detected in the config file!")
	}
	port, _ := strconv.Atoi(v.GetString("port"))
	allTables := strings.Split(strings.ReplaceAll(strings.ReplaceAll(v.GetString("table"), " ", ""), "\\", ""), ",") // replace " " and "\"" by ""
	cfgSectionParts := strings.Split(instance, ".")
	switch cfgSectionParts[0] {
	case "exasol":
		instanceToConfig[instance] = &dbms.ExasolDB{
			Cfg: dbms.Config{Loglevel: logLvl,
				Instance: instance,
				Host:     v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables},
		}
	case "oracle":
		instanceToConfig[instance] = &dbms.OracleDB{
			Cfg: dbms.Config{Loglevel: logLvl,
				Instance: instance,
				Host:     v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables},
			Srv: v.GetString("service"),
		}
	case "mysql":
		instanceToConfig[instance] = &dbms.MysqlDB{
			Cfg: dbms.Config{Loglevel: logLvl,
				Instance: instance,
				Host:     v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables},
		}
	case "postgresql":
		instanceToConfig[instance] = &dbms.PostgresqlDB{
			Cfg: dbms.Config{Loglevel: logLvl,
				Instance: instance,
				Host:     v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables},
			Db: v.GetString("database"),
		}
	case "mssql":
		instanceToConfig[instance] = &dbms.MssqlDB{
			Cfg: dbms.Config{Loglevel: logLvl,
				Instance: instance,
				Host:     v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables,
			},
			Db: v.GetString("database"),
		}
	// CHECK: Add support for other DBMS
	default:
		panic("Something went wrong - this branch shouldn't be reached!")
	}
}

// createFileCheck checks if the specified file can be created
// func createFileCheck(fileName *string) error {
// 	file, err := os.OpenFile(*fileName, os.O_CREATE, 0600)
// 	if err != nil {
// 		// permission denied
// 		return err
// 	}
// 	defer file.Close()

// 	return err
// }

// setupEnv reads the config file and sets the instance config for all active instances
func setupEnv(cfg *string) error {
	var err error

	viper.SetConfigName(filepath.Base(*cfg)) // config file name
	viper.SetConfigType("yaml")              // config file type
	viper.AddConfigPath(filepath.Dir(*cfg))  // config file path
	if err = viper.ReadInConfig(); err != nil {
		return err
	}

	// read common config parameters
	logFile := viper.GetString("Logfile")
	if logFile == "" {
		return errors.New("the Logfile parameter isn't configured")
	} else {
		// err := createFileCheck(&logFile)
		// if err != nil {
		// 	return err
		// }
		// log.LogHandler(logFile)
		// sLog.Startup(100)
		sLog.SetupLog(logFile, true)
	}
	passwordStoreFile = viper.GetString("Passwordstore")
	if passwordStoreFile == "" {
		return errors.New("the Passwordstore parameter isn't configured")
	}
	//else {
	// err := createFileCheck(&passwordStoreFile)
	// if err != nil {
	// 	return err
	// }
	// }

	// read DBMS instance config parameters
	for _, v := range supportedDbms {
		cfgFirstLevelKey := viper.GetStringMapString(v) // all cfg instances (instance1, instance2, ...) are assigned to a DBMS name (exasol, oracle, ...)
		dbmsInstance := ""
		for k := range cfgFirstLevelKey {
			dbmsInstance = v + "." + k // e.g. exasol.instance1
			if cfgInstance := viper.Sub(dbmsInstance); cfgInstance != nil && cfgInstance.GetString("active") == "1" {
				setInstanceConfig(dbmsInstance, cfgInstance)
			}
		}
	}

	return err
}
