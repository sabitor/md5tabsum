package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/sabitor/simplelog"
)

type mssqlDB struct {
	cfg config
	db  string // MSSQL specific
}

func (s *mssqlDB) instance() string {
	return s.cfg.instance
}

func (s *mssqlDB) host() string {
	return s.cfg.host
}

func (s *mssqlDB) port() int {
	return s.cfg.port
}

func (s *mssqlDB) user() string {
	return s.cfg.user
}

func (s *mssqlDB) schema() string {
	return s.cfg.schema
}

func (s *mssqlDB) table() []string {
	return s.cfg.table
}

func (s *mssqlDB) database() string {
	return s.db
}

func (s *mssqlDB) logPrefix() string {
	return "Instance: " + s.instance() + " -"
}

// ----------------------------------------------------------------------------
func (s *mssqlDB) openDB(password string) (*sql.DB, error) {
	tableFilter := strings.Join(s.table(), ", ")
	simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, s.logPrefix(), "Profile parameter:", "Host:"+s.host(), "Port:"+strconv.Itoa(s.port()), "Database:"+s.database(), "User:"+s.user(), "Schema:"+s.schema(), "Table:"+tableFilter)
	dsn := fmt.Sprintf("server=%s;user id=%s; password=%s; port=%d; database=%s;", s.host(), s.user(), password, s.port(), s.database())
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		simplelog.Write(simplelog.MULTI, s.logPrefix(), err.Error())
		return db, err
	}
	return db, err
}

func (s *mssqlDB) closeDB(db *sql.DB) error {
	return db.Close()
}

func (s *mssqlDB) queryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var err error

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range s.table() {
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=@p1 and TABLE_NAME like @p2"
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, s.logPrefix(), "SQL[1]: "+sqlPreparedStmt, "-", "TABLE_SCHEMA:"+s.schema()+",", "TABLE_NAME:"+table)
		rowSet, err = db.Query(sqlPreparedStmt, s.schema(), table)
		if err != nil {
			simplelog.Write(simplelog.MULTI, s.logPrefix(), err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// table exists in DB schema
			err = rowSet.Scan(&foundTable)
			if err != nil {
				simplelog.Write(simplelog.MULTI, s.logPrefix(), err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// table doesn't exist in the DB schema
			err = errors.New("Table " + table + " could not be found.")
			simplelog.Write(simplelog.MULTI, s.logPrefix(), err.Error())
			return err
		}
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=@p1 and TABLE_NAME=@p2 order by ORDINAL_POSITION asc"
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, s.logPrefix(), "SQL[2]: "+sqlPreparedStmt, "-", "TABLE_SCHEMA:"+s.schema()+",", "TABLE_NAME:"+table)
		rowSet, err = db.Query(sqlPreparedStmt, s.schema(), table)
		if err != nil {
			simplelog.Write(simplelog.MULTI, s.logPrefix(), err.Error())
			return err
		}

		var columnNames, column, columnType string
		var ordinalPosition int

		for rowSet.Next() {
			if columnNames != "" {
				columnNames += " + "
			}
			err := rowSet.Scan(&column, &columnType, &ordinalPosition)
			if err != nil {
				simplelog.Write(simplelog.MULTI, s.logPrefix(), err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				// calculate the MD5 of a string-type column to prevent a potential varchar(max) overflow of all concatenated columns
				columnNames += "coalesce(lower(convert(varchar(32), HashBytes('MD5', rtrim(" + column + ")),2)), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "DECIMAL") {
				columnNames += "coalesce(cast(cast(" + column + " as float) as varchar(max)), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(cast(format(" + column + ", 'yyyy-MM-dd HH:mm:ss.ffffff') as varchar(max)), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "FLOAT") {
				columnNames += "coalesce(convert(varchar(max), " + column + ", 128), 'null')"
			} else {
				columnNames += "coalesce(cast(" + column + " as varchar(max)), 'null')"
			}

			simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, s.logPrefix(), "Column", ordinalPosition, "of "+table+":", column, "("+columnType+")")
		}

		// compile MD5 (00000000000000000000000000000000 is the default result for an empty table) by using the following SQL:
		//   select count(1) NUMROWS,
		//          coalesce(lower(convert(varchar(max), HashBytes('MD5', concat(cast(sum(convert(bigint, convert(varbinary, substring(t.ROWHASH, 1,8), 2))) as varchar(max)),
		//                                                              cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 9,8), 2))) as varchar(max)),
		//                                                              cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 17,8), 2))) as varchar(max)),
		//                                                              cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 25,8), 2))) as varchar(max)))),2)),
		//                   '00000000000000000000000000000000') CHECKSUM
		//   from (select lower(convert(varchar(max), HashBytes('MD5', %s), 2)) ROWHASH from %s.%s) t
		sqlText := "select count(1) NUMROWS, coalesce(lower(convert(varchar(max), HashBytes('MD5', cast(sum(convert(bigint, convert(varbinary, substring(t.ROWHASH, 1,8), 2))) as varchar(max)) + cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 9,8), 2))) as varchar(max)) + cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 17,8), 2))) as varchar(max)) + cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 25,8), 2))) as varchar(max))),2)), '00000000000000000000000000000000') CHECKSUM from (select lower(convert(varchar(max), HashBytes('MD5', %s), 2)) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, s.schema(), table)
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, s.logPrefix(), "SQL[3]: "+sqlQueryStmt)

		var numTableRows int
		var checkSum string
		err = db.QueryRow(sqlQueryStmt).Scan(&numTableRows, &checkSum)
		if err != nil {
			simplelog.Write(simplelog.MULTI, s.logPrefix(), err.Error())
			return err
		}
		simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, s.logPrefix(), "Table:"+table+",", "Number of rows:", numTableRows)

		simplelog.Write(simplelog.STDOUT, fmt.Sprintf("%s:%s", s.instance()+"."+table, checkSum))
		simplelog.Write(simplelog.FILE, s.logPrefix(), "Table:"+table+",", "MD5: "+checkSum)
	}

	return err
}
