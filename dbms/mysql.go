package dbms

import (
	"database/sql"
	"fmt"
	"md5tabsum/log"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlDB struct {
	Cfg Config
}

func (m *MysqlDB) LogLevel() int {
	return m.Cfg.Loglevel
}

func (m *MysqlDB) Instance() string {
	return m.Cfg.Instance
}

func (m *MysqlDB) Host() string {
	return m.Cfg.Host
}

func (m *MysqlDB) Port() int {
	return m.Cfg.Port
}

func (m *MysqlDB) User() string {
	return m.Cfg.User
}

func (m *MysqlDB) Schema() string {
	return m.Cfg.Schema
}

func (m *MysqlDB) Table() []string {
	return m.Cfg.Table
}

// ----------------------------------------------------------------------------
func (m *MysqlDB) OpenDB(password string) (*sql.DB, error) {
	sqlMode := "ANSI_QUOTES"
	tableFilter := strings.Join(m.Table(), ", ")
	log.WriteLog(log.MEDIUM, m.LogLevel(), log.LOGFILE, "[Instance]", m.Instance(), "[Host]", m.Host(), "[Port]", strconv.Itoa(m.Port()), "[User]", m.User(), "[Schema]", m.Schema(), "[Table]", tableFilter)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?sql_mode=%s", m.User(), password, m.Host(), m.Port(), m.Schema(), sqlMode)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.WriteLog(log.BASIC, m.LogLevel(), log.BOTH, err.Error())
		return db, err
	}
	return db, err
}

func (m *MysqlDB) CloseDB(db *sql.DB) error {
	return db.Close()
}

func (m *MysqlDB) QueryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// PREPARE: Filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range m.Table() {
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where table_schema=? and table_name like ?"
		rowSet, err = db.Query(sqlPreparedStmt, m.Schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, m.LogLevel(), log.BOTH, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// Table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				log.WriteLog(log.BASIC, m.LogLevel(), log.BOTH, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// Table doesn't exist in the DB schema
			log.WriteLog(log.BASIC, m.LogLevel(), log.BOTH, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: Compile MD5 for all found tables
	maxChar := 65535
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=? and TABLE_NAME=? order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, m.Schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, m.LogLevel(), log.BOTH, err.Error())
			return err
		}

		var numColumns int // required for building the correct 'concat' string
		var columnNames, column, columnType string
		var logColumns, logColumnTypes []string

		// Gather table properties
		for rowSet.Next() {
			if columnNames != "" {
				columnNames += ", "
			} else {
				columnNames += "concat("
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				log.WriteLog(log.BASIC, m.LogLevel(), log.BOTH, err.Error())
				return err
			}

			// Convert all columns into string data type
			if strings.Contains(strings.ToUpper(columnType), "CHAR") {
				columnNames += "coalesce(md5(\"" + column + "\"), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "DECIMAL") {
				columnNames += "coalesce(cast(trim(TRAILING '0' from " + column + ") as char(" + strconv.Itoa(maxChar) + ")), 'null')"
			} else if strings.Contains(strings.ToUpper(columnType), "TIME") || strings.Contains(strings.ToUpper(columnType), "DATE") {
				columnNames += "coalesce(date_format(\"" + column + "\", '%Y-%m-%d %H:%i:%s.%f'), 'null')"
			} else {
				columnNames += "coalesce(cast(\"" + column + "\" as char(" + strconv.Itoa(maxChar) + ")), 'null')"
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
		log.WriteLog(log.FULL, m.LogLevel(), log.LOGFILE, "[COLUMNS]", strings.Join(logColumns, ", "), "[DATATYPES]", strings.Join(logColumnTypes, ", "))

		// Compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table)
		sqlText := "select coalesce(md5(concat(sum(cast(conv(substring(ROWHASH, 1, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 9, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 17, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 25, 8), 16, 10) as unsigned)))), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select md5(%s) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, m.Schema(), table)
		log.WriteLog(log.FULL, m.LogLevel(), log.LOGFILE, "[SQL]", sqlQueryStmt)

		// Start SQL command
		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			log.WriteLog(log.BASIC, m.LogLevel(), log.BOTH, err.Error())
			return err
		}

		result := fmt.Sprintf("%s:%s", m.Instance()+"."+table, checkSum)
		log.WriteLog(log.BASIC, m.LogLevel(), log.LOGFILE, "[Checksum]", result)
		log.WriteLog(log.BASIC, m.LogLevel(), log.STDOUT, result)
	}

	return err
}
