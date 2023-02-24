package dbms

import (
	"database/sql"
	"fmt"
	"md5tabsum/log"
	"strconv"
	"strings"

	go_ora "github.com/sijms/go-ora/v2"
)

type OracleDB struct {
	Cfg Config
	Srv string
}

func (o *OracleDB) LogLevel() int {
	return o.Cfg.Loglevel
}

func (o *OracleDB) Instance() string {
	return o.Cfg.Instance
}

func (o *OracleDB) Host() string {
	return o.Cfg.Host
}

func (o *OracleDB) Port() int {
	return o.Cfg.Port
}

func (o *OracleDB) User() string {
	return o.Cfg.User
}

func (o *OracleDB) Schema() string {
	return o.Cfg.Schema
}

func (o *OracleDB) Table() []string {
	return o.Cfg.Table
}

func (o *OracleDB) Service() string {
	return o.Srv
}

// ----------------------------------------------------------------------------
func (o *OracleDB) OpenDB(password string) (*sql.DB, error) {
	// urlOptions := map[string]string{
	// 	"trace file": "trace.log",
	// }

	tableFilter := strings.Join(o.Table(), ", ")
	log.WriteLog(log.MEDIUM, o.LogLevel(), log.LOGFILE, "[Instance]: "+o.Instance(), "[Host]: "+o.Host(), "[Port]: "+strconv.Itoa(o.Port()), "[Service]: "+o.Service(), "[User]: "+o.User(), "[Schema]: "+o.Schema(), "[Table]: "+tableFilter)
	dsn := go_ora.BuildUrl(o.Host(), o.Port(), o.Service(), o.User(), password /* urlOptions */, nil)
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		log.WriteLog(log.BASIC, o.LogLevel(), log.BOTH, err.Error())
		return db, err
	}
	return db, err
}

func (o *OracleDB) CloseDB(db *sql.DB) error {
	return db.Close()
}

func (o *OracleDB) QueryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// Set '.' as NUMBER/FLOAT decimal point for this session
	_, err = db.Exec("alter session set NLS_NUMERIC_CHARACTERS = '.,'")
	if err != nil {
		log.WriteLog(log.BASIC, o.LogLevel(), log.BOTH, err.Error())
		return err
	}

	// PREPARE: Filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range o.Table() {
		// Hint: Prepared statements are currently not supported by go-ora. Thus, the command will be build by using the real filter values instead of using place holders.
		sqlPreparedStmt := "select TABLE_NAME from ALL_TABLES where OWNER='" + strings.ToUpper(o.Schema()) + "' and TABLE_NAME like '" + strings.ToUpper(table) + "'"
		rowSet, err = db.Query(sqlPreparedStmt)
		if err != nil {
			log.WriteLog(log.BASIC, o.LogLevel(), log.BOTH, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// Table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				log.WriteLog(log.BASIC, o.LogLevel(), log.BOTH, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// Table doesn't exist in the DB schema
			log.WriteLog(log.BASIC, o.LogLevel(), log.BOTH, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: compile MD5 for all found tables
	max := 4000
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE || '(' || DATA_LENGTH || ',' || DATA_PRECISION || ',' || DATA_SCALE || ')' as DATA_TYPE from ALL_TAB_COLS where OWNER='" + strings.ToUpper(o.Schema()) + "' and TABLE_NAME='" + strings.ToUpper(table) + "' order by COLUMN_ID asc"
		rowSet, err = db.Query(sqlPreparedStmt)
		if err != nil {
			log.WriteLog(log.BASIC, o.LogLevel(), log.BOTH, err.Error())
			return err
		}

		var columnNames, column, columnType string
		var logColumns, logColumnTypes []string

		for rowSet.Next() {
			if columnNames != "" {
				columnNames += " || "
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				log.WriteLog(log.BASIC, o.LogLevel(), log.BOTH, err.Error())
				return err
			}

			// Convert all columns into string data type
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

			logColumns = append(logColumns, column)
			logColumnTypes = append(logColumnTypes, columnType)
		}
		log.WriteLog(log.FULL, o.LogLevel(), log.LOGFILE, "[COLUMNS]: "+strings.Join(logColumns, ", "), "[DATATYPES]: "+strings.Join(logColumnTypes, ", "))

		// Compile checksum
		sqlText := "select /*+ PARALLEL */ lower(cast(standard_hash(sum(to_number(substr(t.rowhash, 1, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 9, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 17, 8), 'xxxxxxxx')) || sum(to_number(substr(t.rowhash, 25, 8), 'xxxxxxxx')), 'MD5') as varchar(4000))) CHECKSUM from ( select standard_hash(%s, 'MD5') ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, o.Schema(), table)
		log.WriteLog(log.FULL, o.LogLevel(), log.LOGFILE, "[SQL]: "+sqlQueryStmt)

		// Start SQL command
		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			log.WriteLog(log.BASIC, o.LogLevel(), log.BOTH, err.Error())
			return err
		}

		result := fmt.Sprintf("%s:%s", o.Instance()+"."+table, checkSum)
		log.WriteLog(log.BASIC, o.LogLevel(), log.LOGFILE, "[Checksum]: "+result)
		log.WriteLog(log.BASIC, o.LogLevel(), log.STDOUT, result)
	}

	return err
}
