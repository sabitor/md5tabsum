package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/sijms/go-ora/v2"
	// _ "github.com/godror/godror"
)

type oracleDB struct {
	cfg     config
	service string
}

func (o *oracleDB) Instance() *string {
	return &o.cfg.instance
}

func (o *oracleDB) Host() string {
	return o.cfg.host
}

func (o *oracleDB) Port() int {
	return o.cfg.port
}

func (o *oracleDB) User() string {
	return o.cfg.user
}

func (o *oracleDB) Schema() string {
	return o.cfg.schema
}

func (o *oracleDB) Table() []string {
	return o.cfg.table
}

func (o *oracleDB) Service() string {
	return o.service
}

func (o *oracleDB) ObjId(obj *string) *string {
	objId := o.cfg.instance + "." + *obj
	return &objId
}

// ----------------------------------------------------------------------------
func (o *oracleDB) openDB() (*sql.DB, error) {
	/* urlOptions := map[string]string{
		"trace file": "trace.log",
	} */

	password := gInstancePassword[*o.Instance()]
	tableFilter := strings.Join(o.Table(), ", ")
	writeLog(1, o.Instance(), "Host: "+o.Host(), "Port: "+strconv.Itoa(o.Port()), "Service: "+o.Service(), "User: "+o.User(), "Schema: "+o.Schema(), "Table: "+tableFilter)
	dsn := go_ora.BuildUrl(o.Host(), o.Port(), o.Service(), o.User(), password /* urlOptions */, nil)
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		writeLog(1, o.Instance(), err.Error())
		writeLogBasic(STDOUT, err.Error())
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
	var checkSum string
	var err error

	// Set '.' as NUMBER/FLOAT decimal point for this session
	_, err = db.Exec("alter session set NLS_NUMERIC_CHARACTERS = '.,'")
	if err != nil {
		writeLog(1, o.Instance(), err.Error())
		writeLogBasic(STDOUT, err.Error())
		return err
	}

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	logTableNamesFalse := EMPTYSTRING
	for _, table := range o.Table() {
		// Hint: Prepared statements are currently not supported by go-ora. Thus, the command will be build by using the real filter values instead of using place holders.
		sqlPreparedStmt := "select TABLE_NAME from ALL_TABLES where OWNER='" + strings.ToUpper(o.Schema()) + "' and TABLE_NAME like '" + strings.ToUpper(table) + "'"
		rowSet, err = db.Query(sqlPreparedStmt)
		if err != nil {
			writeLog(1, o.Instance(), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}
		foundTable := EMPTYSTRING
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				writeLog(1, o.Instance(), err.Error())
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
		writeLog(1, o.Instance(), message)
		writeLogBasic(STDOUT, message)
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE || '(' || DATA_LENGTH || ',' || DATA_PRECISION || ',' || DATA_SCALE || ')' as DATA_TYPE from ALL_TAB_COLS where OWNER='" + strings.ToUpper(o.Schema()) + "' and TABLE_NAME='" + strings.ToUpper(table) + "' order by COLUMN_ID asc"
		rowSet, err = db.Query(sqlPreparedStmt)
		if err != nil {
			writeLog(1, o.ObjId(&table), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}

		max := 4000
		columnNames, column, columnType := EMPTYSTRING, EMPTYSTRING, EMPTYSTRING
		// logging
		logColumns, logColumnTypes := EMPTYSTRING, EMPTYSTRING

		for rowSet.Next() {
			if columnNames != EMPTYSTRING {
				columnNames += " || "
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				writeLog(1, o.ObjId(&table), err.Error())
				writeLogBasic(STDOUT, err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
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

			buildLogMessage(&logColumns, &column)
			buildLogMessage(&logColumnTypes, &columnType)
		}
		writeLog(2, o.ObjId(&table), "COLUMNS: "+logColumns, "DATATYPES: "+logColumnTypes)

		// compile checksum
		sqlText := "select /*+ PARALLEL */ lower(cast(standard_hash(sum(to_number(substr(t.rowhash, 1, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 9, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 17, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 25, 8), 'xxxxxxxx')), 'MD5') as varchar(4000))) CHECKSUM from ( select standard_hash(%s, 'MD5') ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, o.Schema(), table)
		writeLog(2, o.ObjId(&table), "SQL: "+sqlQueryStmt)

		// start SQL command
		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			writeLog(1, o.ObjId(&table), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}

		// write checksum to STDOUT and to the log file
		result := fmt.Sprintf("%s:%s", *o.ObjId(&table), checkSum)
		writeLog(1, o.ObjId(&table), result)
		writeLogBasic(STDOUT, result)
	}

	return err
}