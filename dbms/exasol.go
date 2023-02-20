package dbms

import (
	"database/sql"
	"fmt"
	"md5tabsum/constant"
	"strconv"
	"strings"

	"github.com/exasol/exasol-driver-go"
)

type ExasolDB struct {
	Cfg Config
}

func (e *ExasolDB) Instance() *string {
	return &e.Cfg.Instance
}

func (e *ExasolDB) Host() string {
	return e.Cfg.Host
}

func (e *ExasolDB) Port() int {
	return e.Cfg.Port
}

func (e *ExasolDB) User() string {
	return e.Cfg.User
}

func (e *ExasolDB) Schema() string {
	return e.Cfg.Schema
}

func (e *ExasolDB) Table() []string {
	return e.Cfg.Table
}

func (e *ExasolDB) ObjId(obj *string) *string {
	objId := e.Cfg.Instance + "." + *obj
	return &objId
}

// ----------------------------------------------------------------------------
func (e *ExasolDB) OpenDB(password string) (*sql.DB, error) {
	tableFilter := strings.Join(e.Table(), ", ")
	writeLog(1, e.Instance(), "Host: "+e.Host(), "Port: "+strconv.Itoa(e.Port()), "User: "+e.User(), "Schema: "+e.Schema(), "Table: "+tableFilter)
	db, err := sql.Open("exasol", exasol.NewConfig(e.User(), password).Port(e.Port()).Host(e.Host()).ValidateServerCertificate(false).String())
	if err != nil {
		writeLog(1, e.Instance(), err.Error())
		writeLogBasic(constant.STDOUT, err.Error())
		return db, err
	}
	return db, err
}

func (e *ExasolDB) CloseDB(db *sql.DB) error {
	return db.Close()
}

func (e *ExasolDB) QueryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// Set '.' as NUMBER/FLOAT decimal point for this session
	_, err = db.Exec("alter session set NLS_NUMERIC_CHARACTERS = '.,'")
	if err != nil {
		writeLog(1, e.Instance(), err.Error())
		writeLogBasic(constant.STDOUT, err.Error())
		return err
	}

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	logTableNamesFalse := constant.EMPTYSTRING
	for _, table := range e.Table() {
		sqlPreparedStmt := "select TABLE_NAME from EXA_ALL_TABLES where table_schema=? and table_name like ?"
		rowSet, err = db.Query(sqlPreparedStmt, strings.ToUpper(e.Schema()), strings.ToUpper(table))
		if err != nil {
			writeLog(1, e.Instance(), err.Error())
			writeLogBasic(constant.STDOUT, err.Error())
			return err
		}
		foundTable := constant.EMPTYSTRING
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				writeLog(1, e.Instance(), err.Error())
				writeLogBasic(constant.STDOUT, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == constant.EMPTYSTRING {
			// table doesn't exist in the DB schema
			buildLogMessage(&logTableNamesFalse, &table)
		}
	}
	if logTableNamesFalse != constant.EMPTYSTRING {
		message := "Table(s) for filter '" + logTableNamesFalse + "' not found."
		writeLog(1, e.Instance(), message)
		writeLogBasic(constant.STDOUT, message)
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, COLUMN_TYPE from EXA_ALL_COLUMNS where COLUMN_SCHEMA=? and COLUMN_TABLE=? order by COLUMN_ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, strings.ToUpper(e.Schema()), strings.ToUpper(table))
		if err != nil {
			writeLog(1, e.ObjId(&table), err.Error())
			writeLogBasic(constant.STDOUT, err.Error())
			return err
		}

		max := 2000000
		columnNames, column, columnType := constant.EMPTYSTRING, constant.EMPTYSTRING, constant.EMPTYSTRING
		// logging
		logColumns, logColumnTypes := constant.EMPTYSTRING, constant.EMPTYSTRING

		for rowSet.Next() {
			if columnNames != constant.EMPTYSTRING {
				columnNames += " || "
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				writeLog(1, e.ObjId(&table), err.Error())
				writeLogBasic(constant.STDOUT, err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				columnNames += "coalesce(hash_md5(rtrim(\"" + column + "\")), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(to_char(\"" + column + "\", 'YYYY-MM-DD HH24:MI:SS.FF6'), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "BOOLEAN") {
				columnNames += "coalesce(cast(case when \"" + column + "\"=TRUE then 1 else 0 end as varchar(" + strconv.Itoa(max) + ")), 'null')"
			} else {
				columnNames += "coalesce(cast(\"" + column + "\" as varchar(" + strconv.Itoa(max) + ")), 'null')"
			}

			buildLogMessage(&logColumns, &column)
			buildLogMessage(&logColumnTypes, &columnType)
		}
		writeLog(2, e.ObjId(&table), "COLUMNS: "+logColumns, "DATATYPES: "+logColumnTypes)

		// compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table)
		sqlText := "select coalesce(hash_md5(sum(to_number(substr(t.rowhash, 1, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 9, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 17, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 25, 8), 'xxxxxxxx'))), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select hash_md5(%s) ROWHASH from %s.%s) as t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, e.Schema(), table)
		writeLog(2, e.ObjId(&table), "SQL: "+sqlQueryStmt)

		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			writeLog(1, e.ObjId(&table), err.Error())
			writeLogBasic(constant.STDOUT, err.Error())
			return err
		}

		// write checksum to STDOUT and to the log file
		result := fmt.Sprintf("%s:%s", *e.ObjId(&table), checkSum)
		writeLog(1, e.ObjId(&table), result)
		writeLogBasic(constant.STDOUT, result)
	}

	return err
}
