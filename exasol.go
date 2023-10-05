package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/exasol/exasol-driver-go"
	"github.com/sabitor/simplelog"
)

type exasolDB struct {
	cfg config
}

func (e *exasolDB) instance() string {
	return e.cfg.instance
}

func (e *exasolDB) host() string {
	return e.cfg.host
}

func (e *exasolDB) port() int {
	return e.cfg.port
}

func (e *exasolDB) user() string {
	return e.cfg.user
}

func (e *exasolDB) schema() string {
	return e.cfg.schema
}

func (e *exasolDB) table() []string {
	return e.cfg.table
}

func (e *exasolDB) logPrefix() string {
	return "[" + e.instance() + "] -"
}

// ----------------------------------------------------------------------------
func (e *exasolDB) openDB(password string) (*sql.DB, error) {
	tableFilter := strings.Join(e.table(), ", ")
	simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, e.logPrefix(), "Host:"+e.host(), "Port:"+strconv.Itoa(e.port()), "User:"+e.user(), "Schema:"+e.schema(), "Table:"+tableFilter)
	db, err := sql.Open("exasol", exasol.NewConfig(e.user(), password).Port(e.port()).Host(e.host()).ValidateServerCertificate(false).String())
	if err != nil {
		simplelog.Write(simplelog.MULTI, e.logPrefix(), err.Error())
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
	var err error

	// set '.' as NUMBER/FLOAT decimal point for this session
	_, err = db.Exec("alter session set NLS_NUMERIC_CHARACTERS = '.,'")
	if err != nil {
		simplelog.Write(simplelog.MULTI, e.logPrefix(), err.Error())
		return err
	}

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range e.table() {
		sqlPreparedStmt := "select TABLE_NAME from EXA_ALL_TABLES where table_schema=? and table_name like ?"
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, e.logPrefix(), "SQL[1]: "+sqlPreparedStmt, "-", "TABLE_SCHEMA:"+e.schema()+",", "TABLE_NAME:"+table)
		rowSet, err = db.Query(sqlPreparedStmt, strings.ToUpper(e.schema()), strings.ToUpper(table))
		if err != nil {
			simplelog.Write(simplelog.MULTI, e.logPrefix(), err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				simplelog.Write(simplelog.MULTI, e.logPrefix(), err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// table doesn't exist in the DB schema
			err = errors.New("Table " + table + " could not be found.")
			simplelog.Write(simplelog.MULTI, e.logPrefix(), err.Error())
			return err
		}
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, COLUMN_TYPE, COLUMN_ORDINAL_POSITION from EXA_ALL_COLUMNS where COLUMN_SCHEMA=? and COLUMN_TABLE=? order by COLUMN_ORDINAL_POSITION asc"
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, e.logPrefix(), "SQL[2]: "+sqlPreparedStmt, "-", "COLUMN_SCHEMA:"+e.schema()+",", "COLUMN_TABLE:"+table)
		rowSet, err = db.Query(sqlPreparedStmt, strings.ToUpper(e.schema()), strings.ToUpper(table))
		if err != nil {
			simplelog.Write(simplelog.MULTI, e.logPrefix(), err.Error())
			return err
		}

		var columnNames, column, columnType string
		var ordinalPosition int

		// gather table properties
		for rowSet.Next() {
			if columnNames != "" {
				columnNames += " || "
			}
			err := rowSet.Scan(&column, &columnType, &ordinalPosition)
			if err != nil {
				simplelog.Write(simplelog.MULTI, e.logPrefix(), err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				// calculate the MD5 of a string-type column to prevent a potential varchar(max) overflow of all concatenated columns
				columnNames += "coalesce(hash_md5(rtrim(\"" + column + "\")), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(to_char(\"" + column + "\", 'YYYY-MM-DD HH24:MI:SS.FF6'), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "BOOLEAN") {
				columnNames += "coalesce(cast(case when \"" + column + "\"=TRUE then 1 else 0 end as varchar(" + strconv.Itoa(2000000) + ")), 'null')"
			} else {
				columnNames += "coalesce(cast(\"" + column + "\" as varchar(" + strconv.Itoa(2000000) + ")), 'null')"
			}

			simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, e.logPrefix(), "Column", ordinalPosition, "of "+table+":", column, "("+columnType+")")
		}

		// compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table) by using the following SQL:
		//   select count(1) NUMROWS,
		//          coalesce(hash_md5(sum(to_number(substr(t.rowhash, 1, 8), 'xxxxxxxx')) ||
		//                            sum(to_number(substr(t.rowhash, 9, 8), 'xxxxxxxx')) ||
		//                            sum(to_number(substr(t.rowhash, 17, 8), 'xxxxxxxx')) ||
		//                            sum(to_number(substr(t.rowhash, 25, 8), 'xxxxxxxx'))),
		//                   'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM
		//   from (select hash_md5(%s) ROWHASH from %s.%s) as t
		sqlText := "select count(1) NUMROWS, coalesce(hash_md5(sum(to_number(substr(t.rowhash, 1, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 9, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 17, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 25, 8), 'xxxxxxxx'))), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select hash_md5(%s) ROWHASH from %s.%s) as t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, e.schema(), table)
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, e.logPrefix(), "SQL[3]: "+sqlQueryStmt)

		var numTableRows int
		var checkSum string
		err = db.QueryRow(sqlQueryStmt).Scan(&numTableRows, &checkSum)
		if err != nil {
			simplelog.Write(simplelog.MULTI, e.logPrefix(), err.Error())
			return err
		}
		simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, e.logPrefix(), "Table:"+table+",", "Number of rows:", numTableRows)

		simplelog.Write(simplelog.STDOUT, fmt.Sprintf("%s:%s", e.instance()+"."+table, checkSum))
		simplelog.Write(simplelog.FILE, e.logPrefix(), "Table:"+table+",", "MD5: "+checkSum)
	}

	return err
}
