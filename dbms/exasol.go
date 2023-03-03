package dbms

import (
	"database/sql"
	"fmt"
	"md5tabsum/log"
	"strconv"
	"strings"

	"github.com/exasol/exasol-driver-go"
)

// ExasolDB defines the attributes of the Exasol DBMS
type ExasolDB struct {
	Cfg Config
}

func (e *ExasolDB) logLevel() int {
	return e.Cfg.Loglevel
}

func (e *ExasolDB) instance() string {
	return e.Cfg.Instance
}

func (e *ExasolDB) host() string {
	return e.Cfg.Host
}

func (e *ExasolDB) port() int {
	return e.Cfg.Port
}

func (e *ExasolDB) user() string {
	return e.Cfg.User
}

func (e *ExasolDB) schema() string {
	return e.Cfg.Schema
}

func (e *ExasolDB) table() []string {
	return e.Cfg.Table
}

// OpenDB implements the OpenDB method of the DBMS interface
func (e *ExasolDB) OpenDB(password string) (*sql.DB, error) {
	tableFilter := strings.Join(e.table(), ", ")
	log.WriteLog(log.MEDIUM, e.logLevel(), log.LOGFILE, "[Instance]: "+e.instance(), "[Host]: "+e.host(), "[Port]: "+strconv.Itoa(e.port()), "[User]: "+e.user(), "[Schema]: "+e.schema(), "[Table]: "+tableFilter)
	db, err := sql.Open("exasol", exasol.NewConfig(e.user(), password).Port(e.port()).Host(e.host()).ValidateServerCertificate(false).String())
	if err != nil {
		log.WriteLog(log.BASIC, e.logLevel(), log.BOTH, err.Error())
		return db, err
	}
	return db, err
}

// CloseDB implements the CloseDB method of the DBMS interface
func (e *ExasolDB) CloseDB(db *sql.DB) error {
	return db.Close()
}

// QueryDB implements the QueryDB method of the DBMS interface
func (e *ExasolDB) QueryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// Set '.' as NUMBER/FLOAT decimal point for this session
	_, err = db.Exec("alter session set NLS_NUMERIC_CHARACTERS = '.,'")
	if err != nil {
		log.WriteLog(log.MEDIUM, e.logLevel(), log.BOTH, err.Error())
		return err
	}

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range e.table() {
		sqlPreparedStmt := "select TABLE_NAME from EXA_ALL_TABLES where table_schema=? and table_name like ?"
		rowSet, err = db.Query(sqlPreparedStmt, strings.ToUpper(e.schema()), strings.ToUpper(table))
		if err != nil {
			log.WriteLog(log.BASIC, e.logLevel(), log.BOTH, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// Table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				log.WriteLog(log.BASIC, e.logLevel(), log.BOTH, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// Table doesn't exist in the DB schema
			log.WriteLog(log.BASIC, e.logLevel(), log.BOTH, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: compile MD5 for all found tables
	max := 2000000
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, COLUMN_TYPE from EXA_ALL_COLUMNS where COLUMN_SCHEMA=? and COLUMN_TABLE=? order by COLUMN_ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, strings.ToUpper(e.schema()), strings.ToUpper(table))
		if err != nil {
			log.WriteLog(log.BASIC, e.logLevel(), log.BOTH, err.Error())
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
				log.WriteLog(log.BASIC, e.logLevel(), log.BOTH, err.Error())
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
		log.WriteLog(log.FULL, e.logLevel(), log.LOGFILE, "[COLUMNS]: "+strings.Join(logColumns, ", "), "[DATATYPES]: "+strings.Join(logColumnTypes, ", "))

		// Compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table)
		sqlText := "select coalesce(hash_md5(sum(to_number(substr(t.rowhash, 1, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 9, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 17, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 25, 8), 'xxxxxxxx'))), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select hash_md5(%s) ROWHASH from %s.%s) as t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, e.schema(), table)
		log.WriteLog(log.FULL, e.logLevel(), log.LOGFILE, "[SQL]: "+sqlQueryStmt)

		// Start SQL command
		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			log.WriteLog(log.BASIC, e.logLevel(), log.BOTH, err.Error())
			return err
		}

		result := fmt.Sprintf("%s:%s", e.instance()+"."+table, checkSum)
		log.WriteLog(log.BASIC, e.logLevel(), log.LOGFILE, "[Checksum]: "+result)
		log.WriteLog(log.BASIC, e.logLevel(), log.STDOUT, result)
	}

	return err
}
