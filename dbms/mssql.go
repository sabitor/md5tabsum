package dbms

import (
	"database/sql"
	"fmt"
	"md5tabsum/constant"
	"md5tabsum/log"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

type MssqlDB struct {
	Cfg Config
	Db  string
}

func (s *MssqlDB) Instance() *string {
	return &s.Cfg.Instance
}

func (s *MssqlDB) Host() string {
	return s.Cfg.Host
}

func (s *MssqlDB) Port() int {
	return s.Cfg.Port
}

func (s *MssqlDB) User() string {
	return s.Cfg.User
}

func (s *MssqlDB) Schema() string {
	return s.Cfg.Schema
}

func (s *MssqlDB) Table() []string {
	return s.Cfg.Table
}

func (s *MssqlDB) Database() string {
	return s.Db
}

func (s *MssqlDB) ObjId(obj *string) *string {
	objId := s.Cfg.Instance + "." + *obj
	return &objId
}

// ----------------------------------------------------------------------------
func (s *MssqlDB) OpenDB(password string) (*sql.DB, error) {
	tableFilter := strings.Join(s.Table(), ", ")
	log.WriteLog(1, s.Instance(), "Host: "+s.Host(), "Port: "+strconv.Itoa(s.Port()), "Database: "+s.Database(), "User: "+s.User(), "Schema: "+s.Schema(), "Table: "+tableFilter)
	dsn := fmt.Sprintf("server=%s;user id=%s; password=%s; port=%d; database=%s;", s.Host(), s.User(), password, s.Port(), s.Database())
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.WriteLog(1, s.Instance(), err.Error())
		log.WriteLogBasic(constant.STDOUT, err.Error())
		return db, err
	}
	return db, err
}

func (s *MssqlDB) CloseDB(db *sql.DB) error {
	return db.Close()
}

func (s *MssqlDB) QueryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	logTableNamesFalse := constant.EMPTYSTRING
	for _, table := range s.Table() {
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=@p1 and TABLE_NAME like @p2"
		rowSet, err = db.Query(sqlPreparedStmt, s.Schema(), table)
		if err != nil {
			log.WriteLog(1, s.Instance(), err.Error())
			log.WriteLogBasic(constant.STDOUT, err.Error())
			return err
		}
		foundTable := constant.EMPTYSTRING
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				log.WriteLog(1, s.Instance(), err.Error())
				log.WriteLogBasic(constant.STDOUT, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == constant.EMPTYSTRING {
			// table doesn't exist in the DB schema
			log.BuildLogMessage(&logTableNamesFalse, &table)
		}
	}
	if logTableNamesFalse != constant.EMPTYSTRING {
		message := "Table(s) for filter '" + logTableNamesFalse + "' not found."
		log.WriteLog(1, s.Instance(), message)
		log.WriteLogBasic(constant.STDOUT, message)
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=@p1 and TABLE_NAME=@p2 order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, s.Schema(), table)
		if err != nil {
			log.WriteLog(1, s.ObjId(&table), err.Error())
			log.WriteLogBasic(constant.STDOUT, err.Error())
			return err
		}

		columnNames, column, columnType := constant.EMPTYSTRING, constant.EMPTYSTRING, constant.EMPTYSTRING
		numColumns := 0 // required for building the correct 'concat' string
		// logging
		logColumns, logColumnTypes := constant.EMPTYSTRING, constant.EMPTYSTRING

		for rowSet.Next() {
			if columnNames != constant.EMPTYSTRING {
				columnNames += ", "
			} else {
				columnNames += "concat("
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				log.WriteLog(1, s.ObjId(&table), err.Error())
				log.WriteLogBasic(constant.STDOUT, err.Error())
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
			log.BuildLogMessage(&logColumns, &column)
			log.BuildLogMessage(&logColumnTypes, &columnType)
		}
		if numColumns > 1 {
			columnNames += ")"
		} else {
			columnNames += ", 'null')"
		}
		log.WriteLog(2, s.ObjId(&table), "COLUMNS: "+logColumns, "DATATYPES: "+logColumnTypes)

		// compile checksum
		sqlText := "select lower(convert(varchar(max), HashBytes('MD5', concat(cast(sum(convert(bigint, convert(varbinary, substring(t.ROWHASH, 1,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 9,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 17,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 25,8), 2))) as varchar(max)))),2)) CHECKSUM from (select lower(convert(varchar(max), HashBytes('MD5', %s), 2)) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, s.Schema(), table)
		log.WriteLog(2, s.ObjId(&table), "SQL: "+sqlQueryStmt)

		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			log.WriteLog(1, s.ObjId(&table), err.Error())
			log.WriteLogBasic(constant.STDOUT, err.Error())
			return err
		}

		// write checksum to STDOUT and to the log file
		result := fmt.Sprintf("%s:%s", *s.ObjId(&table), checkSum)
		log.WriteLog(1, s.ObjId(&table), result)
		log.WriteLogBasic(constant.STDOUT, result)
	}

	return err
}
