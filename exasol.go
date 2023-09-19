package main

import (
	"database/sql"
	"fmt"
	"md5tabsum/log"
	"strconv"
	"strings"

	"github.com/exasol/exasol-driver-go"
)

type exasolDB struct {
	cfg config
}

func (e *exasolDB) LogLevel() int {
	return e.cfg.loglevel
}

func (e *exasolDB) Instance() string {
	return e.cfg.instance
}

func (e *exasolDB) Host() string {
	return e.cfg.host
}

func (e *exasolDB) Port() int {
	return e.cfg.port
}

func (e *exasolDB) User() string {
	return e.cfg.user
}

func (e *exasolDB) Schema() string {
	return e.cfg.schema
}

func (e *exasolDB) Table() []string {
	return e.cfg.table
}

// ----------------------------------------------------------------------------
func (e *exasolDB) openDB(password string) (*sql.DB, error) {
	tableFilter := strings.Join(e.Table(), ", ")
	log.WriteLog(log.MEDIUM, e.LogLevel(), log.LOGFILE, "[Instance]: "+e.Instance(), "[Host]: "+e.Host(), "[Port]: "+strconv.Itoa(e.Port()), "[User]: "+e.User(), "[Schema]: "+e.Schema(), "[Table]: "+tableFilter)
	db, err := sql.Open("exasol", exasol.NewConfig(e.User(), password).Port(e.Port()).Host(e.Host()).ValidateServerCertificate(false).String())
	if err != nil {
		log.WriteLog(log.BASIC, e.LogLevel(), log.BOTH, err.Error())
		return db, err
	}
	return db, err
}

func (e *exasolDB) closeDB(db *sql.DB) error {
	return db.Close()
}

func (e *exasolDB) queryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// Set '.' as NUMBER/FLOAT decimal point for this session
	_, err = db.Exec("alter session set NLS_NUMERIC_CHARACTERS = '.,'")
	if err != nil {
		log.WriteLog(log.MEDIUM, e.LogLevel(), log.BOTH, err.Error())
		return err
	}

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range e.Table() {
		sqlPreparedStmt := "select TABLE_NAME from EXA_ALL_TABLES where table_schema=? and table_name like ?"
		rowSet, err = db.Query(sqlPreparedStmt, strings.ToUpper(e.Schema()), strings.ToUpper(table))
		if err != nil {
			log.WriteLog(log.BASIC, e.LogLevel(), log.BOTH, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// Table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				log.WriteLog(log.BASIC, e.LogLevel(), log.BOTH, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// Table doesn't exist in the DB schema
			log.WriteLog(log.BASIC, e.LogLevel(), log.BOTH, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: compile MD5 for all found tables
	max := 2000000
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, COLUMN_TYPE from EXA_ALL_COLUMNS where COLUMN_SCHEMA=? and COLUMN_TABLE=? order by COLUMN_ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, strings.ToUpper(e.Schema()), strings.ToUpper(table))
		if err != nil {
			log.WriteLog(log.BASIC, e.LogLevel(), log.BOTH, err.Error())
			return err
		}

		var columnNames, column, columnType string
		var logColumns, logColumnTypes []string

		// Gather table properties
		for rowSet.Next() {
			if columnNames != "" {
				columnNames += " || "
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				log.WriteLog(log.BASIC, e.LogLevel(), log.BOTH, err.Error())
				return err
			}

			// Convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				columnNames += "coalesce(hash_md5(rtrim(\"" + column + "\")), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(to_char(\"" + column + "\", 'YYYY-MM-DD HH24:MI:SS.FF6'), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "BOOLEAN") {
				columnNames += "coalesce(cast(case when \"" + column + "\"=TRUE then 1 else 0 end as varchar(" + strconv.Itoa(max) + ")), 'null')"
			} else {
				columnNames += "coalesce(cast(\"" + column + "\" as varchar(" + strconv.Itoa(max) + ")), 'null')"
			}

			logColumns = append(logColumns, column)
			logColumnTypes = append(logColumnTypes, columnType)
		}
		log.WriteLog(log.FULL, e.LogLevel(), log.LOGFILE, "[COLUMNS]: "+strings.Join(logColumns, ", "), "[DATATYPES]: "+strings.Join(logColumnTypes, ", "))

		// Compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table)
		sqlText := "select coalesce(hash_md5(sum(to_number(substr(t.rowhash, 1, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 9, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 17, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 25, 8), 'xxxxxxxx'))), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select hash_md5(%s) ROWHASH from %s.%s) as t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, e.Schema(), table)
		log.WriteLog(log.FULL, e.LogLevel(), log.LOGFILE, "[SQL]: "+sqlQueryStmt)

		// Start SQL command
		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			log.WriteLog(log.BASIC, e.LogLevel(), log.BOTH, err.Error())
			return err
		}

		result := fmt.Sprintf("%s:%s", e.Instance()+"."+table, checkSum)
		log.WriteLog(log.BASIC, e.LogLevel(), log.LOGFILE, "[Checksum]: "+result)
		log.WriteLog(log.BASIC, e.LogLevel(), log.STDOUT, result)
	}

	return err
}
