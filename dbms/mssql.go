package dbms

import (
	"database/sql"
	"fmt"
	"md5tabsum/log"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

type MssqlDB struct {
	Cfg Config
	Db  string
}

func (s *MssqlDB) LogLevel() int {
	return s.Cfg.Loglevel
}

func (s *MssqlDB) Instance() string {
	return s.Cfg.Instance
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

// ----------------------------------------------------------------------------
func (s *MssqlDB) OpenDB(password string) (*sql.DB, error) {
	tableFilter := strings.Join(s.Table(), ", ")
	log.WriteLog(log.MEDIUM, s.LogLevel(), log.LOGFILE, "[Instance]: "+s.Instance(), "[Host]: "+s.Host(), "[Port]: "+strconv.Itoa(s.Port()), "[Database]: "+s.Database(), "[User]: "+s.User(), "[Schema]: "+s.Schema(), "[Table]: "+tableFilter)
	dsn := fmt.Sprintf("server=%s;user id=%s; password=%s; port=%d; database=%s;", s.Host(), s.User(), password, s.Port(), s.Database())
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		log.WriteLog(log.BASIC, s.LogLevel(), log.BOTH, err.Error())
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
	for _, table := range s.Table() {
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA=@p1 and TABLE_NAME like @p2"
		rowSet, err = db.Query(sqlPreparedStmt, s.Schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, s.LogLevel(), log.BOTH, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				log.WriteLog(log.BASIC, s.LogLevel(), log.BOTH, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// table doesn't exist in the DB schema
			log.WriteLog(log.BASIC, s.LogLevel(), log.BOTH, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=@p1 and TABLE_NAME=@p2 order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, s.Schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, s.LogLevel(), log.BOTH, err.Error())
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
				log.WriteLog(log.BASIC, s.LogLevel(), log.BOTH, err.Error())
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
		log.WriteLog(log.FULL, s.LogLevel(), log.LOGFILE, "[Columns]: "+strings.Join(logColumns, ", "), "[Datatypes]: "+strings.Join(logColumnTypes, ", "))

		// compile checksum
		sqlText := "select lower(convert(varchar(max), HashBytes('MD5', concat(cast(sum(convert(bigint, convert(varbinary, substring(t.ROWHASH, 1,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 9,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 17,8), 2))) as varchar(max)), cast(sum(convert(bigint, convert(VARBINARY, substring(t.ROWHASH, 25,8), 2))) as varchar(max)))),2)) CHECKSUM from (select lower(convert(varchar(max), HashBytes('MD5', %s), 2)) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, s.Schema(), table)
		log.WriteLog(log.FULL, s.LogLevel(), log.LOGFILE, "[SQL]: "+sqlQueryStmt)

		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			log.WriteLog(log.BASIC, s.LogLevel(), log.BOTH, err.Error())
			return err
		}

		result := fmt.Sprintf("%s:%s", s.Instance()+"."+table, checkSum)
		log.WriteLog(log.BASIC, s.LogLevel(), log.LOGFILE, "[Checksum]: "+result)
		log.WriteLog(log.BASIC, s.LogLevel(), log.STDOUT, result)
	}

	return err
}
