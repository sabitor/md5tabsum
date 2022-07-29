package main

import (
	"errors"
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
	allTables := strings.Split(strings.ReplaceAll(strings.ReplaceAll(v.GetString("table"), " ", EMPTYSTRING), "\\", EMPTYSTRING), ",") // replace " " and "\"" by ""
	cfgSectionParts := strings.Split(cfgSection, ".")                                                                                  // e.g. exasol.instance1
	switch cfgSectionParts[0] {
	case "exasol":
		gDbms[cfgSection] = &exasolDB{
			cfg: config{host: v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables,
				instance: cfgSection},
		}
		gInstanceLogLevel[cfgSection] = logLevel
	case "oracle":
		gDbms[cfgSection] = &oracleDB{
			cfg: config{host: v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables,
				instance: cfgSection},
			service: v.GetString("service"),
		}
		gInstanceLogLevel[cfgSection] = logLevel
	case "mysql":
		gDbms[cfgSection] = &mysqlDB{
			cfg: config{host: v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables,
				instance: cfgSection},
		}
		gInstanceLogLevel[cfgSection] = logLevel
	case "postgresql":
		gDbms[cfgSection] = &postgresqlDB{
			cfg: config{host: v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables,
				instance: cfgSection},
			database: v.GetString("database"),
		}
		gInstanceLogLevel[cfgSection] = logLevel
	case "mssql":
		gDbms[cfgSection] = &mssqlDB{
			cfg: config{host: v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables,
				instance: cfgSection},
			database: v.GetString("database"),
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
	if logFile == EMPTYSTRING {
		return errors.New("the Logfile parameter isn't configured")
	} else {
		err := createFileCheck(&logFile)
		if err != nil {
			return err
		}
		LogHandler(logFile)
	}
	passwordStore := viper.GetString("Passwordstore")
	if passwordStore == EMPTYSTRING {
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
		dbmsInstance := EMPTYSTRING
		for k := range cfgFirstLevelKey {
			dbmsInstance = v + "." + k // e.g. exasol.instance1
			if cfgInstance := viper.Sub(dbmsInstance); cfgInstance != nil && cfgInstance.GetString("active") == "1" {
				setInstanceConfig(dbmsInstance, cfgInstance)
			}
		}
	}

	return err
}
