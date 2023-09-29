package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sabitor/simplelog"
)

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

func (m *mysqlDB) logPrefix() string {
	return "Instance: " + m.instance() + " -"
}

// ----------------------------------------------------------------------------
func (m *mysqlDB) openDB(password string) (*sql.DB, error) {
	sqlMode := "ANSI_QUOTES"
	tableFilter := strings.Join(m.table(), ", ")
	simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, m.logPrefix(), "Profile parameter:", "DBHost:"+m.host()+",", "Port:"+strconv.Itoa(m.port())+",", "User:"+m.user()+",", "Schema:"+m.schema()+",", "Table:"+tableFilter)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?sql_mode=%s", m.user(), password, m.host(), m.port(), m.schema(), sqlMode)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		simplelog.Write(simplelog.MULTI, m.logPrefix(), err.Error())
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
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=? and TABLE_NAME like ?"
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, m.logPrefix(), "SQL[1]: "+sqlPreparedStmt, "-", "TABLE_SCHEMA:"+m.schema()+",", "TABLE_NAME:"+table)
		rowSet, err = db.Query(sqlPreparedStmt, m.schema(), table)
		if err != nil {
			simplelog.Write(simplelog.MULTI, m.logPrefix(), err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				simplelog.Write(simplelog.MULTI, m.logPrefix(), err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// table doesn't exist in the DB schema
			err = errors.New("Table " + table + " could not be found.")
			simplelog.Write(simplelog.MULTI, m.logPrefix(), err.Error())
			return err
		}
	}

	// EXECUTE: Compile MD5 for all found tables
	maxChar := 65535
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=? and TABLE_NAME=? order by ORDINAL_POSITION asc"
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, m.logPrefix(), "SQL[2]: "+sqlPreparedStmt, "-", "TABLE_SCHEMA:"+m.schema()+",", "TABLE_NAME:"+table)
		rowSet, err = db.Query(sqlPreparedStmt, m.schema(), table)
		if err != nil {
			simplelog.Write(simplelog.MULTI, m.logPrefix(), err.Error())
			return err
		}

		var columnNames, column, columnType string
		var ordinalPosition int

		// gather table properties
		for rowSet.Next() {
			if columnNames != "" {
				columnNames += ", "
			}
			err := rowSet.Scan(&column, &columnType, &ordinalPosition)
			if err != nil {
				simplelog.Write(simplelog.MULTI, m.logPrefix(), err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				// calculate the MD5 of a string-type column to prevent a potential varchar(max) overflow of all concatenated columns
				columnNames += "coalesce(md5(" + column + "), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "DECIMAL") {
				columnNames += "coalesce(cast(trim(TRAILING '0' from " + column + ") as char(" + strconv.Itoa(maxChar) + ")), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(date_format(" + column + ", '%Y-%m-%d %H:%i:%s.%f'), 'null')"
			} else {
				columnNames += "coalesce(cast(" + column + " as char(" + strconv.Itoa(maxChar) + ")), 'null')"
			}

			simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, m.logPrefix(), "Column", ordinalPosition, "of "+table+":", column, "("+columnType+")")
		}
		if ordinalPosition > 1 {
			// table contains more than one column - concatenate them
			columnNames = "concat(" + columnNames + ")"
		}

		// compile MD5 (00000000000000000000000000000000 is the default result for an empty table) by using the following SQL:
		//   select count(1) NUMROWS,
		//          coalesce(md5(concat(sum(cast(conv(substring(ROWHASH, 1, 8), 16, 10) as unsigned)),
		//                              sum(cast(conv(substring(ROWHASH, 9, 8), 16, 10) as unsigned)),
		//                              sum(cast(conv(substring(ROWHASH, 17, 8), 16, 10) as unsigned)),
		//                              sum(cast(conv(substring(ROWHASH, 25, 8), 16, 10) as unsigned)))),
		//                   '00000000000000000000000000000000') CHECKSUM
		//   from (select md5(%s) ROWHASH from %s.%s) t
		sqlText := "select count(1) NUMROWS, coalesce(md5(concat(sum(cast(conv(substring(ROWHASH, 1, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 9, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 17, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 25, 8), 16, 10) as unsigned)))), '00000000000000000000000000000000') CHECKSUM from (select md5(%s) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, m.schema(), table)
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, m.logPrefix(), "SQL[3]: "+sqlQueryStmt)

		var numTableRows int
		var checkSum string
		err = db.QueryRow(sqlQueryStmt).Scan(&numTableRows, &checkSum)
		if err != nil {
			simplelog.Write(simplelog.MULTI, m.logPrefix(), err.Error())
			return err
		}
		simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, m.logPrefix(), "Table:"+table+",", "Number of rows:", numTableRows)

		simplelog.Write(simplelog.STDOUT, fmt.Sprintf("%s:%s", m.instance()+"."+table, checkSum))
		simplelog.Write(simplelog.FILE, m.logPrefix(), "Table:"+table+",", "MD5: "+checkSum)
	}

	return err
}
