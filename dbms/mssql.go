package dbms

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

type mssqlDB struct {
	cfg      config
	database string
}

func (s *mssqlDB) Instance() *string {
	return &s.cfg.instance
}

func (s *mssqlDB) Host() string {
	return s.cfg.host
}

func (s *mssqlDB) Port() int {
	return s.cfg.port
}

func (s *mssqlDB) User() string {
	return s.cfg.user
}

func (s *mssqlDB) Schema() string {
	return s.cfg.schema
}

func (s *mssqlDB) Table() []string {
	return s.cfg.table
}

func (s *mssqlDB) Database() string {
	return s.database
}

func (s *mssqlDB) ObjId(obj *string) *string {
	objId := s.cfg.instance + "." + *obj
	return &objId
}

// ----------------------------------------------------------------------------
func (s *mssqlDB) openDB() (*sql.DB, error) {
	password := gInstancePassword[*s.Instance()]
	tableFilter := strings.Join(s.Table(), ", ")
	writeLog(1, s.Instance(), "Host: "+s.Host(), "Port: "+strconv.Itoa(s.Port()), "Database: "+s.Database(), "User: "+s.User(), "Schema: "+s.Schema(), "Table: "+tableFilter)
	dsn := fmt.Sprintf("server=%s;user id=%s; password=%s; port=%d; database=%s;", s.Host(), s.User(), password, s.Port(), s.Database())
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		writeLog(1, s.Instance(), err.Error())
		writeLogBasic(STDOUT, err.Error())
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
	var checkSum string
	var err error

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	logTableNamesFalse := EMPTYSTRING
	for _, table := range s.Table() {
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=@p1 and TABLE_NAME like @p2"
		rowSet, err = db.Query(sqlPreparedStmt, s.Schema(), table)
		if err != nil {
			writeLog(1, s.Instance(), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}
		foundTable := EMPTYSTRING
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				writeLog(1, s.Instance(), err.Error())
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
		writeLog(1, s.Instance(), message)
		writeLogBasic(STDOUT, message)
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=@p1 and TABLE_NAME=@p2 order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, s.Schema(), table)
		if err != nil {
			writeLog(1, s.ObjId(&table), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}

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
				writeLog(1, s.ObjId(&table), err.Error())
				writeLogBasic(STDOUT, err.Error())
				return err
			}

			// convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				columnNames += "coalesce(lower(convert(varchar(32), HashBytes('MD5', rtrim(\"" + column + "\")),2)), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "DECIMAL") {
				columnNames += "coalesce(cast(cast(\"" + column + "\" as float) as varchar(max)), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(cast(format(\"" + column + "\", 'yyyy-MM-dd HH:mm:ss.ffffff') as varchar(max)), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "FLOAT") {
				columnNames += "coalesce(convert(varchar(max), \"" + column + "\", 128), 'null')"
			} else {
				columnNames += "coalesce(cast(\"" + column + "\" as varchar(max)), 'null')"
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
		writeLog(2, s.ObjId(&table), "COLUMNS: "+logColumns, "DATATYPES: "+logColumnTypes)

		// compile checksum
		sqlText := "select lower(convert(varchar(max), HashBytes('MD5', concat(cast(sum(convert(bigint, convert(varbinary, substring(t.ROWHASH, 1,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 9,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 17,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 25,8), 2))) as varchar(max)))),2)) CHECKSUM from (select lower(convert(varchar(max), HashBytes('MD5', %s), 2)) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, s.Schema(), table)
		writeLog(2, s.ObjId(&table), "SQL: "+sqlQueryStmt)

		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			writeLog(1, s.ObjId(&table), err.Error())
			writeLogBasic(STDOUT, err.Error())
			return err
		}

		// write checksum to STDOUT and to the log file
		result := fmt.Sprintf("%s:%s", *s.ObjId(&table), checkSum)
		writeLog(1, s.ObjId(&table), result)
		writeLogBasic(STDOUT, result)
	}

	return err
}
