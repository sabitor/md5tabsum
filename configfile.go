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

// setInstanceConfig sets the instance parameters according the parsed config file section
func setInstanceConfig(cfgSection string, v *viper.Viper) {
	logLevel, _ := strconv.Atoi(v.GetString("loglevel"))
	port, _ := strconv.Atoi(v.GetString("port"))
	allTables := strings.Split(strings.ReplaceAll(strings.ReplaceAll(v.GetString("table"), " ", constant.EMPTYSTRING), "\\", constant.EMPTYSTRING), ",") // replace " " and "\"" by ""
	cfgSectionParts := strings.Split(cfgSection, ".")                                                                                                    // e.g. exasol.instance1
	switch cfgSectionParts[0] {
	case "exasol":
		gDbms[cfgSection] = &dbms.ExasolDB{
			Cfg: dbms.Config{Host: v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables,
				Instance: cfgSection},
		}
		gInstanceLogLevel[cfgSection] = logLevel
	case "oracle":
		gDbms[cfgSection] = &dbms.OracleDB{
			Cfg: dbms.Config{Host: v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables,
				Instance: cfgSection},
			Srv: v.GetString("service"),
		}
		gInstanceLogLevel[cfgSection] = logLevel
	case "mysql":
		gDbms[cfgSection] = &dbms.MysqlDB{
			Cfg: dbms.Config{Host: v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables,
				Instance: cfgSection},
		}
		gInstanceLogLevel[cfgSection] = logLevel
	case "postgresql":
		gDbms[cfgSection] = &dbms.PostgresqlDB{
			Cfg: dbms.Config{Host: v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables,
				Instance: cfgSection},
			Db: v.GetString("database"),
		}
		gInstanceLogLevel[cfgSection] = logLevel
	case "mssql":
		gDbms[cfgSection] = &dbms.MssqlDB{
			Cfg: dbms.Config{Host: v.GetString("host"),
				Port:     port,
				User:     v.GetString("user"),
				Schema:   v.GetString("schema"),
				Table:    allTables,
				Instance: cfgSection},
			Db: v.GetString("database"),
		}
		gInstanceLogLevel[cfgSection] = logLevel
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
	for _, v := range gSupportedDbms {
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
