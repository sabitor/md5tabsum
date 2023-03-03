package dbms

import (
	"database/sql"
	"fmt"
	"md5tabsum/log"
	"strconv"
	"strings"

	// Import of the MySQL driver to be used by the database/sql API
	_ "github.com/go-sql-driver/mysql"
)

// MysqlDB defines the attributes of the MySQL DBMS
type MysqlDB struct {
	Cfg Config
}

func (m *MysqlDB) logLevel() int {
	return m.Cfg.Loglevel
}

func (m *MysqlDB) instance() string {
	return m.Cfg.Instance
}

func (m *MysqlDB) host() string {
	return m.Cfg.Host
}

func (m *MysqlDB) port() int {
	return m.Cfg.Port
}

func (m *MysqlDB) user() string {
	return m.Cfg.User
}

func (m *MysqlDB) schema() string {
	return m.Cfg.Schema
}

func (m *MysqlDB) table() []string {
	return m.Cfg.Table
}

// OpenDB implements the OpenDB method of the DBMS interface
func (m *MysqlDB) OpenDB(password string) (*sql.DB, error) {
	sqlMode := "ANSI_QUOTES"
	tableFilter := strings.Join(m.table(), ", ")
	log.WriteLog(log.MEDIUM, m.logLevel(), log.LOGFILE, "[Instance]: "+m.instance(), "[Host]: "+m.host(), "[Port]: "+strconv.Itoa(m.port()), "[User]: "+m.user(), "[Schema]: "+m.schema(), "[Table]: "+tableFilter)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?sql_mode=%s", m.user(), password, m.host(), m.port(), m.schema(), sqlMode)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.WriteLog(log.BASIC, m.logLevel(), log.BOTH, err.Error())
		return db, err
	}
	return db, err
}

// CloseDB implements the CloseDB method of the DBMS interface
func (m *MysqlDB) CloseDB(db *sql.DB) error {
	return db.Close()
}

// QueryDB implements the QueryDB method of the DBMS interface
func (m *MysqlDB) QueryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// PREPARE: Filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range m.table() {
		sqlPreparedStmt := "select TABLE_NAME from INFORMATION_SCHEMA.TABLES where table_schema=? and table_name like ?"
		rowSet, err = db.Query(sqlPreparedStmt, m.schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, m.logLevel(), log.BOTH, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// Table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				log.WriteLog(log.BASIC, m.logLevel(), log.BOTH, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// Table doesn't exist in the DB schema
			log.WriteLog(log.BASIC, m.logLevel(), log.BOTH, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: Compile MD5 for all found tables
	maxChar := 65535
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=? and TABLE_NAME=? order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, m.schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, m.logLevel(), log.BOTH, err.Error())
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
				log.WriteLog(log.BASIC, m.logLevel(), log.BOTH, err.Error())
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
		log.WriteLog(log.FULL, m.logLevel(), log.LOGFILE, "[COLUMNS]: "+strings.Join(logColumns, ", "), "[DATATYPES]: "+strings.Join(logColumnTypes, ", "))

		// Compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table)
		sqlText := "select coalesce(md5(concat(sum(cast(conv(substring(ROWHASH, 1, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 9, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 17, 8), 16, 10) as unsigned)), sum(cast(conv(substring(ROWHASH, 25, 8), 16, 10) as unsigned)))), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select md5(%s) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, m.schema(), table)
		log.WriteLog(log.FULL, m.logLevel(), log.LOGFILE, "[SQL]: "+sqlQueryStmt)

		// Start SQL command
		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			log.WriteLog(log.BASIC, m.logLevel(), log.BOTH, err.Error())
			return err
		}

		result := fmt.Sprintf("%s:%s", m.instance()+"."+table, checkSum)
		log.WriteLog(log.BASIC, m.logLevel(), log.LOGFILE, "[Checksum]: "+result)
		log.WriteLog(log.BASIC, m.logLevel(), log.STDOUT, result)
	}

	return err
}
