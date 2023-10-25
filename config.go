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
	instanceConfig = make(map[string]database) // store config file instances and their assigned configuration
	instanceActive = make(map[string]bool)     // store active config file instances
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
		instanceConfig[instance] = &exasolDB{
			cfg: config{
				instance: instance,
				host:     v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables},
		}
	case "oracle":
		instanceConfig[instance] = &oracleDB{
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
		instanceConfig[instance] = &mysqlDB{
			cfg: config{
				instance: instance,
				host:     v.GetString("host"),
				port:     port,
				user:     v.GetString("user"),
				schema:   v.GetString("schema"),
				table:    allTables},
		}
	case "postgresql":
		instanceConfig[instance] = &postgresqlDB{
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
		instanceConfig[instance] = &mssqlDB{
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
func setupEnv(cfg string) error {
	var err error

	viper.SetConfigName(filepath.Base(cfg)) // config file name
	viper.SetConfigType("yaml")             // config file type
	viper.AddConfigPath(filepath.Dir(cfg))  // config file path
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

	passwordStoreKeyFile = viper.GetString("Passwordstorekey")
	if passwordStoreKeyFile == "" {
		return errors.New(mm015)
	}

	// read DBMS instance config parameters
	supportedDbms := []string{"exasol", "mysql", "mssql", "oracle", "postgresql"}
	instanceKeywords := map[string]struct{}{"active": {}, "host": {}, "port": {}, "user": {}, "database": {}, "schema": {}, "service": {}, "table": {}}
	for _, v := range supportedDbms {
		cfgFirstLevelKey := viper.GetStringMapString(v) // all cfg instances (instance1, instance2, ...) are assigned to a DBMS name (mysql, oracle, ...)
		for k := range cfgFirstLevelKey {
			if _, isInstanceID := instanceKeywords[k]; isInstanceID {
				return errors.New(formatMsg(mm017, v))
			}
			dbmsInstance := v + "." + k // e.g. mysql.instance1
			if cfgInstance := viper.Sub(dbmsInstance); cfgInstance != nil {
				if cfgInstance.GetString("active") == "1" {
					instanceActive[dbmsInstance] = true
				}
				setInstanceConfig(dbmsInstance, cfgInstance)
			}
		}
	}

	return err
}
