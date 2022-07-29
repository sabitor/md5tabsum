package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type mysqlDB struct {
	cfg config
}

func (m *mysqlDB) Instance() *string {
	return &m.cfg.instance
}

func (m *mysqlDB) Host() string {
	return m.cfg.host
}

func (m *mysqlDB) Port() int {
	return m.cfg.port
}

func (m *mysqlDB) User() string {
	return m.cfg.user
}

func (m *mysqlDB) Schema() string {
	return m.cfg.schema
}

func (m *mysqlDB) Table() []string {
	return m.cfg.table
}

func (m *mysqlDB) ObjId(obj *string) *string {
	objId := m.cfg.instance + "." + *obj
	return &objId
}

// ----------------------------------------------------------------------------
func (m *mysqlDB) openDB() (*sql.DB, error) {
	sqlMode := "ANSI_QUOTES"
	password := gInstancePassword[*m.Instance()]
	tableFilter := strings.Join(m.Table(), ", ")
	writeLog(1, m.Instance(), "Host: "+m.Host(), "Port: "+strconv.Itoa(m.Port()), "User: "+m.User(), "Schema: "+m.Schema(), "Table: "+tableFilter)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?sql_mode=%s", m.User(), password, m.Host(), m.Port(), m.Schema(), sqlMode)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		writeLog(1, m.Instance(), err.Error())
		writeLogBasic(STDOUT, err.Error())
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
	var checkSum string
	var err error

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	logTableNamesFalse := EMPTYSTRING
	for _, table := range m.Table() {
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where table_schema=? and table_name like ?"
		rowSet, err = db.Query(sqlPreparedStmt, m.Schema(), table)
		if err != nil {
			writeLog(1, m.Instance(), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}
		foundTable := EMPTYSTRING
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				writeLog(1, m.Instance(), err.Error())
				writeLogBasic(STDOUT, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == EMPTYSTRING {
			// table doesn't exist in the DB schema
			buildLogMessage(&logTableNamesFalse, &table)
		}
	}
	if logTableNamesFalse != EMPTYSTRING {
		message := "Table(s) for filter '" + logTableNamesFalse + "' not found."
		writeLog(1, m.Instance(), message)
		writeLogBasic(STDOUT, message)
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=? and TABLE_NAME=? order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, m.Schema(), table)
		if err != nil {
			writeLog(1, m.ObjId(&table), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}

		max := 65535
		columnNames, column, columnType := EMPTYSTRING, EMPTYSTRING, EMPTYSTRING
		numColumns := 0 // required for building the correct 'concat' string
		// logging
		logColumns, logColumnTypes := EMPTYSTRING, EMPTYSTRING

		for rowSet.Next() {
			if columnNames != EMPTYSTRING {
				columnNames += ", "
			} else {
				columnNames += "concat("
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				writeLog(1, m.ObjId(&table), err.Error())
				writeLogBasic(STDOUT, err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				columnNames += "coalesce(md5(\"" + column + "\"), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "DECIMAL") {
				columnNames += "coalesce(cast(trim(TRAILING '0' from " + column + ") as char(" + strconv.Itoa(max) + ")), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(date_format(\"" + column + "\", '%Y-%m-%d %H:%i:%s.%f'), 'null')"
			} else {
				columnNames += "coalesce(cast(\"" + column + "\" as char(" + strconv.Itoa(max) + ")), 'null')"
			}

			numColumns += 1
			buildLogMessage(&logColumns, &column)
			buildLogMessage(&logColumnTypes, &columnType)
		}
		if numColumns > 1 {
			columnNames += ")"
		} else {
			columnNames += ", 'null')"
		}
		writeLog(2, m.ObjId(&table), "COLUMNS: "+logColumns, "DATATYPES: "+logColumnTypes)

		// compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table)
		sqlText := "select coalesce(md5(concat(sum(cast(conv(substring(ROWHASH, 1, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 9, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 17, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 25, 8), 16, 10) as unsigned)))), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select md5(%s) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, m.Schema(), table)
		writeLog(2, m.ObjId(&table), "SQL: "+sqlQueryStmt)

		// start SQL command
		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			writeLog(1, m.ObjId(&table), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}

		// write checksum to STDOUT and to the log file
		result := fmt.Sprintf("%s:%s", *m.ObjId(&table), checkSum)
		writeLog(1, m.ObjId(&table), result)
		writeLogBasic(STDOUT, result)
	}

	return err
}
