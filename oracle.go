package main

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/sabitor/simplelog"
	go_ora "github.com/sijms/go-ora/v2"
)

type oracleDB struct {
	cfg config
	srv string // Oracle specific
}

func (o *oracleDB) instance() string {
	return o.cfg.instance
}

func (o *oracleDB) host() string {
	return o.cfg.host
}

func (o *oracleDB) port() int {
	return o.cfg.port
}

func (o *oracleDB) user() string {
	return o.cfg.user
}

func (o *oracleDB) schema() string {
	return o.cfg.schema
}

func (o *oracleDB) table() []string {
	return o.cfg.table
}

func (o *oracleDB) service() string {
	return o.srv
}

func (o *oracleDB) logPrefix() string {
	return "[" + o.instance() + "] -"
}

// ----------------------------------------------------------------------------
func (o *oracleDB) openDB(password string) (*sql.DB, error) {
	// urlOptions := map[string]string{
	// 	"trace file": "trace.log",
	// }

	tableFilter := strings.Join(o.table(), ", ")
	simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, o.logPrefix(), "DBHost:"+o.host(), "Port:"+strconv.Itoa(o.port()), "Service:"+o.service(), "User:"+o.user(), "Schema:"+o.schema(), "Table: "+tableFilter)
	dsn := go_ora.BuildUrl(o.host(), o.port(), o.service(), o.user(), password /* urlOptions */, nil)
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		simplelog.Write(simplelog.MULTI, o.logPrefix(), err.Error())
		return db, err
	}
	return db, err
}

func (o *oracleDB) closeDB(db *sql.DB) error {
	return db.Close()
}

func (o *oracleDB) queryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var err error

	// set '.' as NUMBER/FLOAT decimal point for this session
	_, err = db.Exec("alter session set NLS_NUMERIC_CHARACTERS = '.,'")
	if err != nil {
		simplelog.Write(simplelog.MULTI, o.logPrefix(), err.Error())
		return err
	}

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range o.table() {
		// Hint: Prepared statements are currently not supported by go-ora. Thus, the command will be build by using the real filter values instead of using place holders.
		sqlPreparedStmt := "select TABLE_NAME from ALL_TABLES where OWNER='" + strings.ToUpper(o.schema()) + "' and TABLE_NAME like '" + strings.ToUpper(table) + "'"
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, o.logPrefix(), "SQL[1]: "+sqlPreparedStmt)
		rowSet, err = db.Query(sqlPreparedStmt)
		if err != nil {
			simplelog.Write(simplelog.MULTI, o.logPrefix(), err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				simplelog.Write(simplelog.MULTI, o.logPrefix(), err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// table doesn't exist in the DB schema
			err = errors.New("Table " + table + " could not be found.")
			simplelog.Write(simplelog.MULTI, o.logPrefix(), err.Error())
			return err
		}
	}

	// EXECUTE: compile MD5 for all found tables
	max := 4000
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE || '(' || DATA_LENGTH || ',' || coalesce(to_char(DATA_PRECISION), 'na') || ',' || coalesce(to_char(DATA_SCALE), 'na') || ')' as DATA_TYPE, COLUMN_ID from ALL_TAB_COLS where OWNER='" + strings.ToUpper(o.schema()) + "' and TABLE_NAME='" + strings.ToUpper(table) + "' order by COLUMN_ID asc"
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, o.logPrefix(), "SQL[2]: "+sqlPreparedStmt)
		rowSet, err = db.Query(sqlPreparedStmt)
		if err != nil {
			simplelog.Write(simplelog.MULTI, o.logPrefix(), err.Error())
			return err
		}

		var columnNames, column, columnType string
		var columnId int

		// gather table properties
		for rowSet.Next() {
			if columnNames != "" {
				columnNames += " || "
			}
			err := rowSet.Scan(&column, &columnType, &columnId)
			if err != nil {
				simplelog.Write(simplelog.MULTI, o.logPrefix(), err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				// calculate the MD5 of a string-type column to prevent a potential varchar(max) overflow of all concatenated columns
				columnNames += "case when \"" + column + "\" is NULL then 'null' else cast(lower(standard_hash(trim(trailing ' ' from \"" + column + "\"), 'MD5')) as varchar2(4000)) end"
			} else if strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(to_char(\"" + column + "\", 'YYYY-MM-DD HH24:MI:SS')||'.000000', 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") {
				columnNames += "coalesce(to_char(\"" + column + "\", 'YYYY-MM-DD HH24:MI:SS.FF6'), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "NUMBER") || strings.Contains(strings.ToUpper(columnType), "FLOAT") {
				// Hint: Numbers with a leading 0 require special handling (numbers between -1 and 1).
				//       to_char or cast to varchar removes leading 0 from numbers, e.g. 0.123 becomes .123 or -0.123 becomes -.123
				columnNames += "coalesce(case when \"" + column + "\" < 1 and \"" + column + "\" > -1 then rtrim(to_char(\"" + column + "\", 'FM0.9999999999999999999999999'), '.') else to_char(\"" + column + "\") end, 'null')"
			} else {
				columnNames += "coalesce(cast(\"" + column + "\" as varchar2(" + strconv.Itoa(max) + ")), 'null')"
			}

			simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, o.logPrefix(), "Column", columnId, "of "+table+":", column, "("+columnType+")")
		}

		// compile MD5 (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table) by using the following SQL:
		//   select /*+ PARALLEL */
		//          count(1) NUMROWS,
		//          lower(cast(standard_hash(sum(to_number(substr(t.rowhash, 1, 8), 'xxxxxxxx')) ||
		//                                   sum(to_number(substr(t.rowhash, 9, 8), 'xxxxxxxx')) ||
		//                                   sum(to_number(substr(t.rowhash, 17, 8), 'xxxxxxxx')) ||
		//                                   sum(to_number(substr(t.rowhash, 25, 8), 'xxxxxxxx')), 'MD5') as varchar(4000))) CHECKSUM
		//   from (select standard_hash(%s, 'MD5') ROWHASH from %s.%s) t
		sqlText := "select /*+ PARALLEL */ count(1) NUMROWS, lower(cast(standard_hash(sum(to_number(substr(t.rowhash, 1, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 9, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 17, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 25, 8), 'xxxxxxxx')), 'MD5') as varchar(4000))) CHECKSUM from (select standard_hash(%s, 'MD5') ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, o.schema(), table)
		simplelog.ConditionalWrite(condition(pr.logLevel, trace), simplelog.FILE, o.logPrefix(), "SQL[3]: "+sqlQueryStmt)

		var numTableRows int
		var checkSum string
		err = db.QueryRow(sqlQueryStmt).Scan(&numTableRows, &checkSum)
		if err != nil {
			simplelog.Write(simplelog.MULTI, o.logPrefix(), err.Error())
			return err
		}
		simplelog.ConditionalWrite(condition(pr.logLevel, debug), simplelog.FILE, o.logPrefix(), "Table:"+table+",", "Number of rows:", numTableRows)

		simplelog.Write(simplelog.STDOUT, fmt.Sprintf("%s:%s", o.instance()+"."+table, checkSum))
		simplelog.Write(simplelog.FILE, o.logPrefix(), "Table:"+table+",", "MD5: "+checkSum)
	}

	return err
}
