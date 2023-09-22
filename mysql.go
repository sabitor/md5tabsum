package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sabitor/simplelog"
)

var mysqlLogPrefix string

type mysqlDB struct {
	cfg config
}

func (m *mysqlDB) instance() string {
	return m.cfg.instance
}

func (m *mysqlDB) host() string {
	return m.cfg.host
}

func (m *mysqlDB) port() int {
	return m.cfg.port
}

func (m *mysqlDB) user() string {
	return m.cfg.user
}

func (m *mysqlDB) schema() string {
	return m.cfg.schema
}

func (m *mysqlDB) table() []string {
	return m.cfg.table
}

// ----------------------------------------------------------------------------
func (m *mysqlDB) openDB(password string) (*sql.DB, error) {
	sqlMode := "ANSI_QUOTES"
	mysqlLogPrefix = "Instance: " + m.instance() + " -"
	tableFilter := strings.Join(m.table(), ", ")
	simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, mysqlLogPrefix, "DBHost:"+m.host()+",", "Port:"+strconv.Itoa(m.port())+",", "User:"+m.user()+",", "Schema:"+m.schema()+",", "Table:"+tableFilter)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?sql_mode=%s", m.user(), password, m.host(), m.port(), m.schema(), sqlMode)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		simplelog.Write(simplelog.MULTI, err.Error())
		return db, err
	}
	return db, err
}

func (m *mysqlDB) closeDB(db *sql.DB) error {
	return db.Close()
}

func (m *mysqlDB) queryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var err error

	// PREPARE: Filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range m.table() {
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where table_schema=? and table_name like ?"
		rowSet, err = db.Query(sqlPreparedStmt, m.schema(), table)
		if err != nil {
			simplelog.Write(simplelog.MULTI, mysqlLogPrefix, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				simplelog.Write(simplelog.MULTI, mysqlLogPrefix, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// table doesn't exist in the DB schema
			simplelog.Write(simplelog.MULTI, mysqlLogPrefix, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: Compile MD5 for all found tables
	maxChar := 65535
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=? and TABLE_NAME=? order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, m.schema(), table)
		if err != nil {
			simplelog.Write(simplelog.MULTI, mysqlLogPrefix, err.Error())
			return err
		}

		var columnNames, column, columnType string
		ordinalPosition := 1

		// gather table properties
		for rowSet.Next() {
			if columnNames != "" {
				columnNames += ", "
			} else {
				columnNames += "concat("
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				simplelog.Write(simplelog.MULTI, mysqlLogPrefix, err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				columnNames += "coalesce(md5(\"" + column + "\"), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "DECIMAL") {
				columnNames += "coalesce(cast(trim(TRAILING '0' from " + column + ") as char(" + strconv.Itoa(maxChar) + ")), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(date_format(\"" + column + "\", '%Y-%m-%d %H:%i:%s.%f'), 'null')"
			} else {
				columnNames += "coalesce(cast(\"" + column + "\" as char(" + strconv.Itoa(maxChar) + ")), 'null')"
			}

			simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, mysqlLogPrefix, "Column", ordinalPosition, "of "+table+":", column, "("+columnType+")")
			ordinalPosition++
		}
		if ordinalPosition > 1 {
			columnNames += ")"
		} else {
			columnNames += ", 'null')"
		}

		// compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table) by using the following SQL:
		//   select count(1) NUMROWS,
		//          coalesce(md5(concat(sum(cast(conv(substring(ROWHASH, 1, 8), 16, 10) as unsigned)),
		//                              sum(cast(conv(substring(ROWHASH, 9, 8), 16, 10) as unsigned)),
		//                              sum(cast(conv(substring(ROWHASH, 17, 8), 16, 10) as unsigned)),
		//                              sum(cast(conv(substring(ROWHASH, 25, 8), 16, 10) as unsigned)))),
		//                   'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM
		//   from (select md5(%s) ROWHASH from %s.%s) t
		sqlText := "select count(1) NUMROWS, coalesce(md5(concat(sum(cast(conv(substring(ROWHASH, 1, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 9, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 17, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 25, 8), 16, 10) as unsigned)))), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select md5(%s) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, m.schema(), table)
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, mysqlLogPrefix, "SQL: "+sqlQueryStmt)

		var numTableRows int
		var checkSum string
		err = db.QueryRow(sqlQueryStmt).Scan(&numTableRows, &checkSum)
		if err != nil {
			simplelog.Write(simplelog.MULTI, mysqlLogPrefix, err.Error())
			return err
		}
		simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, mysqlLogPrefix, "Table:"+table+",", "Number of rows:", numTableRows)

		simplelog.Write(simplelog.STDOUT, fmt.Sprintf("%s:%s", m.instance()+"."+table, checkSum))
		simplelog.Write(simplelog.FILE, mysqlLogPrefix, "Table:"+table+",", "MD5: "+checkSum)
	}

	return err
}
