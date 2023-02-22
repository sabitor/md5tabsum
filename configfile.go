package main

import (
	"errors"
	"md5tabsum/constant"
	"md5tabsum/dbms"
	"md5tabsum/log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

var (
	// map to store active instances and their assigned configuration
	// mDbms[Key -> DBMS instance name : Value -> DBMS instance config]
	// Example: mDbms["exasol.instance1":exasolDB DBMS interface]
	instanceToConfig = make(map[string]dbms.Database)
	// list of all supported DBMS
	supportedDbms = []string{"exasol", "mysql", "mssql", "oracle", "postgresql"}
)

// setInstanceConfig sets the instance parameters according the parsed config file section
func setInstanceConfig(instance string, v *viper.Viper) {
	logLvl, _ := strconv.Atoi(v.GetString("loglevel"))
	port, _ := strconv.Atoi(v.GetString("port"))
	allTables := strings.Split(strings.ReplaceAll(strings.ReplaceAll(v.GetString("table"), " ", constant.EMPTYSTRING), "\\", constant.EMPTYSTRING), ",") // replace " " and "\"" by ""
	cfgSectionParts := strings.Split(instance, ".")
	switch cfgSectionParts[0] {
	// case "exasol":
	// 	instanceToConfig[instance] = &dbms.ExasolDB{
	// 		Cfg: dbms.Config{Host: v.GetString("host"),
	// 			Port:     port,
	// 			User:     v.GetString("user"),
	// 			Schema:   v.GetString("schema"),
	// 			Table:    allTables,
	// 			Instance: instance},
	// 	}
	// 	log.InstanceToLogLevel[instance] = logLevel
	// case "oracle":
	// 	instanceToConfig[instance] = &dbms.OracleDB{
	// 		Cfg: dbms.Config{Host: v.GetString("host"),
	// 			Port:     port,
	// 			User:     v.GetString("user"),
	// 			Schema:   v.GetString("schema"),
	// 			Table:    allTables,
	// 			Instance: instance},
	// 		Srv: v.GetString("service"),
	// 	}
	// 	log.InstanceToLogLevel[instance] = logLevel
	case "mysql":
		instanceToConfig[instance] = &dbms.MysqlDB{
			Cfg: dbms.Config{Host: v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables,
				Instance: instance,
				Loglevel: logLvl},
		}
		// log.InstanceToLogLevel[instance] = logLevel
	// case "postgresql":
	// 	instanceToConfig[instance] = &dbms.PostgresqlDB{
	// 		Cfg: dbms.Config{Host: v.GetString("host"),
	// 			Port:     port,
	// 			User:     v.GetString("user"),
	// 			Schema:   v.GetString("schema"),
	// 			Table:    allTables,
	// 			Instance: instance},
	// 		Db: v.GetString("database"),
	// 	}
	// 	log.InstanceToLogLevel[instance] = logLevel
	// case "mssql":
	// 	instanceToConfig[instance] = &dbms.MssqlDB{
	// 		Cfg: dbms.Config{Host: v.GetString("host"),
	// 			Port:     port,
	// 			User:     v.GetString("user"),
	// 			Schema:   v.GetString("schema"),
	// 			Table:    allTables,
	// 			Instance: instance},
	// 		Db: v.GetString("database"),
	// 	}
	// 	log.InstanceToLogLevel[instance] = logLevel
	// CHECK: Add support for other DBMS
	default:
		panic("something went wrong - this branch shouldn't be reached")
	}
}

// createFileCheck checks if the specified file can be created
func createFileCheck(fileName *string) error {
	file, err := os.OpenFile(*fileName, os.O_CREATE, 0600)
	if err != nil {
		// permission denied
		return err
	}
	defer file.Close()

	return err
}

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
	if logFile == constant.EMPTYSTRING {
		return errors.New("the Logfile parameter isn't configured")
	} else {
		err := createFileCheck(&logFile)
		if err != nil {
			return err
		}
		log.LogHandler(logFile)
	}
	passwordStore := viper.GetString("Passwordstore")
	if passwordStore == constant.EMPTYSTRING {
		return errors.New("the Passwordstore parameter isn't configured")
	} else {
		err := createFileCheck(&passwordStore)
		if err != nil {
			return err
		}
		gPasswordStore = passwordStore
	}

	// read DBMS instance config parameters
	for _, v := range supportedDbms {
		cfgFirstLevelKey := viper.GetStringMapString(v) // all cfg instances (instance1, instance2, ...) are assigned to a DBMS name (exasol, oracle, ...)
		dbmsInstance := constant.EMPTYSTRING
		for k := range cfgFirstLevelKey {
			dbmsInstance = v + "." + k // e.g. exasol.instance1
			if cfgInstance := viper.Sub(dbmsInstance); cfgInstance != nil && cfgInstance.GetString("active") == "1" {
				setInstanceConfig(dbmsInstance, cfgInstance)
			}
		}
	}

	return err
}
