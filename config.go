package main

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/sabitor/simplelog"
	"github.com/spf13/viper"
)

var (
	// store active instances and their assigned configuration: Key -> DBMS instance name : Value -> DBMS instance config
	instanceToConfig = make(map[string]database)
	// list of all supported DBMS
	supportedDbms = []string{"exasol", "mysql", "mssql", "oracle", "postgresql"}
)

// collection of DBMS config attributes
type config struct {
	instance string
	host     string
	port     int
	user     string
	schema   string
	table    []string
}

// setInstanceConfig sets the instance parameters according the parsed config file section
func setInstanceConfig(instance string, v *viper.Viper) {
	port, _ := strconv.Atoi(v.GetString("port"))
	allTables := strings.Split(strings.ReplaceAll(strings.ReplaceAll(v.GetString("table"), " ", ""), "\\", ""), ",") // replace " " and "\"" by ""
	cfgSectionParts := strings.Split(instance, ".")
	switch cfgSectionParts[0] {
	case "exasol":
		instanceToConfig[instance] = &exasolDB{
			cfg: config{
				instance: instance,
				host:     v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables},
		}
	case "oracle":
		instanceToConfig[instance] = &oracleDB{
			cfg: config{
				instance: instance,
				host:     v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables},
			srv: v.GetString("service"),
		}
	case "mysql":
		instanceToConfig[instance] = &mysqlDB{
			cfg: config{
				instance: instance,
				host:     v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables},
		}
	case "postgresql":
		instanceToConfig[instance] = &postgresqlDB{
			cfg: config{
				instance: instance,
				host:     v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables},
			db: v.GetString("database"),
		}
	case "mssql":
		instanceToConfig[instance] = &mssqlDB{
			cfg: config{
				instance: instance,
				host:     v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables,
			},
			db: v.GetString("database"),
		}
	// CHECK: Add support for other DBMS
	default:
		panic(mm012)
	}
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
	if logFile == "" {
		return errors.New(mm013)
	}
	simplelog.SetupLog(logFile, true)

	passwordStoreFile = viper.GetString("Passwordstore")
	if passwordStoreFile == "" {
		return errors.New(mm014)
	}

	// read DBMS instance config parameters
	for _, v := range supportedDbms {
		cfgFirstLevelKey := viper.GetStringMapString(v) // all cfg instances (instance1, instance2, ...) are assigned to a DBMS name (mysql, oracle, ...)
		dbmsInstance := ""
		for k := range cfgFirstLevelKey {
			dbmsInstance = v + "." + k // e.g. mysql.instance1
			if cfgInstance := viper.Sub(dbmsInstance); cfgInstance != nil && cfgInstance.GetString("active") == "1" {
				setInstanceConfig(dbmsInstance, cfgInstance)
			}
		}
	}

	return err
}
