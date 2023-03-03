package dbms

import (
	"database/sql"
	"fmt"
	"md5tabsum/log"
	"strconv"
	"strings"

	// Import of the MS SQL Server driver to be used by the database/sql API
	_ "github.com/denisenkom/go-mssqldb"
)

// MssqlDB defines the attributes of the MS SQL Server DBMS
type MssqlDB struct {
	Cfg Config
	Db  string
}

func (s *MssqlDB) logLevel() int {
	return s.Cfg.Loglevel
}

func (s *MssqlDB) instance() string {
	return s.Cfg.Instance
}

func (s *MssqlDB) host() string {
	return s.Cfg.Host
}

func (s *MssqlDB) port() int {
	return s.Cfg.Port
}

func (s *MssqlDB) user() string {
	return s.Cfg.User
}

func (s *MssqlDB) schema() string {
	return s.Cfg.Schema
}

func (s *MssqlDB) table() []string {
	return s.Cfg.Table
}

func (s *MssqlDB) database() string {
	return s.Db
}

// OpenDB implements the OpenDB method of the DBMS interface
func (s *MssqlDB) OpenDB(password string) (*sql.DB, error) {
	tableFilter := strings.Join(s.table(), ", ")
	log.WriteLog(log.MEDIUM, s.logLevel(), log.LOGFILE, "[Instance]: "+s.instance(), "[Host]: "+s.host(), "[Port]: "+strconv.Itoa(s.port()), "[Database]: "+s.database(), "[User]: "+s.user(), "[Schema]: "+s.schema(), "[Table]: "+tableFilter)
	dsn := fmt.Sprintf("server=%s;user id=%s; password=%s; port=%d; database=%s;", s.host(), s.user(), password, s.port(), s.database())
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.WriteLog(log.BASIC, s.logLevel(), log.BOTH, err.Error())
		return db, err
	}
	return db, err
}

// CloseDB implements the CloseDB method of the DBMS interface
func (s *MssqlDB) CloseDB(db *sql.DB) error {
	return db.Close()
}

// QueryDB implements the QueryDB method of the DBMS interface
func (s *MssqlDB) QueryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range s.table() {
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=@p1 and TABLE_NAME like @p2"
		rowSet, err = db.Query(sqlPreparedStmt, s.schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, s.logLevel(), log.BOTH, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				log.WriteLog(log.BASIC, s.logLevel(), log.BOTH, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// table doesn't exist in the DB schema
			log.WriteLog(log.BASIC, s.logLevel(), log.BOTH, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=@p1 and TABLE_NAME=@p2 order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, s.schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, s.logLevel(), log.BOTH, err.Error())
			return err
		}

		var numColumns int // required for building the correct 'concat' string
		var columnNames, column, columnType string
		var logColumns, logColumnTypes []string

		for rowSet.Next() {
			if columnNames != "" {
				columnNames += ", "
			} else {
				columnNames += "concat("
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				log.WriteLog(log.BASIC, s.logLevel(), log.BOTH, err.Error())
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

			logColumns = append(logColumns, column)
			logColumnTypes = append(logColumnTypes, columnType)
			numColumns++
		}
		if numColumns > 1 {
			columnNames += ")"
		} else {
			columnNames += ", 'null')"
		}
		log.WriteLog(log.FULL, s.logLevel(), log.LOGFILE, "[Columns]: "+strings.Join(logColumns, ", "), "[Datatypes]: "+strings.Join(logColumnTypes, ", "))

		// compile checksum
		sqlText := "select lower(convert(varchar(max), HashBytes('MD5', concat(cast(sum(convert(bigint, convert(varbinary, substring(t.ROWHASH, 1,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 9,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 17,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 25,8), 2))) as varchar(max)))),2)) CHECKSUM from (select lower(convert(varchar(max), HashBytes('MD5', %s), 2)) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, s.schema(), table)
		log.WriteLog(log.FULL, s.logLevel(), log.LOGFILE, "[SQL]: "+sqlQueryStmt)

		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			log.WriteLog(log.BASIC, s.logLevel(), log.BOTH, err.Error())
			return err
		}

		result := fmt.Sprintf("%s:%s", s.instance()+"."+table, checkSum)
		log.WriteLog(log.BASIC, s.logLevel(), log.LOGFILE, "[Checksum]: "+result)
		log.WriteLog(log.BASIC, s.logLevel(), log.STDOUT, result)
	}

	return err
}
