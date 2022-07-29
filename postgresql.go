package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

type postgresqlDB struct {
	cfg      config
	database string
}

func (p *postgresqlDB) Instance() *string {
	return &p.cfg.instance
}

func (p *postgresqlDB) Host() string {
	return p.cfg.host
}

func (p *postgresqlDB) Port() int {
	return p.cfg.port
}

func (p *postgresqlDB) User() string {
	return p.cfg.user
}

func (p *postgresqlDB) Schema() string {
	return p.cfg.schema
}

func (p *postgresqlDB) Table() []string {
	return p.cfg.table
}

func (p *postgresqlDB) Database() string {
	return p.database
}

func (p *postgresqlDB) ObjId(obj *string) *string {
	objId := p.cfg.instance + "." + *obj
	return &objId
}

// ----------------------------------------------------------------------------
func (p *postgresqlDB) openDB() (*sql.DB, error) {
	password := gInstancePassword[*p.Instance()]
	tableFilter := strings.Join(p.Table(), ", ")
	writeLog(1, p.Instance(), "Host: "+p.Host(), "Port: "+strconv.Itoa(p.Port()), "Database: "+p.Database(), "User: "+p.User(), "Schema: "+p.Schema(), "Table: "+tableFilter)
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", p.Host(), p.Port(), p.User(), password, p.Database())
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		writeLog(1, p.Instance(), err.Error())
		writeLogBasic(STDOUT, err.Error())
		return db, err
	}
	return db, err
}

func (p *postgresqlDB) closeDB(db *sql.DB) error {
	return db.Close()
}

func (p *postgresqlDB) queryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	logTableNamesFalse := EMPTYSTRING
	for _, table := range p.Table() {
		sqlPreparedStmt := "select TABLE_NAME from information_schema.tables where table_schema=$1 and table_name like $2"
		rowSet, err = db.Query(sqlPreparedStmt, p.Schema(), table)
		if err != nil {
			writeLog(1, p.Instance(), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}
		foundTable := EMPTYSTRING
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				writeLog(1, p.Instance(), err.Error())
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
		writeLog(1, p.Instance(), message)
		writeLogBasic(STDOUT, message)
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from information_schema.columns where table_schema=$1 and table_name=$2 order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, p.Schema(), table)
		if err != nil {
			writeLog(1, p.ObjId(&table), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}

		columnNames, column, columnType := EMPTYSTRING, EMPTYSTRING, EMPTYSTRING
		// logging
		logColumns, logColumnTypes := EMPTYSTRING, EMPTYSTRING

		for rowSet.Next() {
			if columnNames != EMPTYSTRING {
				columnNames += " || "
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				writeLog(1, p.ObjId(&table), err.Error())
				writeLogBasic(STDOUT, err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				columnNames += "coalesce(md5(\"" + column + "\"), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "NUMERIC") {
				columnNames += "coalesce(trim_scale(\"" + column + "\")::text, 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(to_char(\"" + column + "\", 'YYYY-MM-DD HH24:MI:SS.US'), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "BOOLEAN") {
				columnNames += "coalesce(\"" + column + "\"::integer::text, 'null')"
			} else {
				columnNames += "coalesce(\"" + column + "\"::text, 'null')"
			}

			buildLogMessage(&logColumns, &column)
			buildLogMessage(&logColumnTypes, &columnType)
		}
		writeLog(2, p.ObjId(&table), "COLUMNS: "+logColumns, "DATATYPES: "+logColumnTypes)

		// compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table)
		sqlText := "select coalesce(md5(sum(('x' || substring(ROWHASH, 1, 8))::bit(32)::bigint)::text || sum(('x' || substring(ROWHASH, 9, 8))::bit(32)::bigint)::text ||sum(('x' || substring(ROWHASH, 17, 8))::bit(32)::bigint)::text || sum(('x' || substring(ROWHASH, 25, 8))::bit(32)::bigint)::text), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select md5(%s) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, p.Schema(), table)
		writeLog(2, p.ObjId(&table), "SQL: "+sqlQueryStmt)

		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			writeLog(1, p.ObjId(&table), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}

		// write checksum to STDOUT and to the log file
		result := fmt.Sprintf("%s:%s", *p.ObjId(&table), checkSum)
		writeLog(1, p.ObjId(&table), result)
		writeLogBasic(STDOUT, result)
	}

	return err
}
