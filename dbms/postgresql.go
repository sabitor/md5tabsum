package dbms

import (
	"database/sql"
	"fmt"
	"md5tabsum/log"
	"strconv"
	"strings"

	// Import of the PostgreSQL driver to be used by the database/sql API
	_ "github.com/lib/pq"
)

// PostgresqlDB defines the attributes of the PostgreSQL DBMS
type PostgresqlDB struct {
	Cfg Config
	Db  string
}

func (p *PostgresqlDB) logLevel() int {
	return p.Cfg.Loglevel
}

func (p *PostgresqlDB) instance() string {
	return p.Cfg.Instance
}

func (p *PostgresqlDB) host() string {
	return p.Cfg.Host
}

func (p *PostgresqlDB) port() int {
	return p.Cfg.Port
}

func (p *PostgresqlDB) user() string {
	return p.Cfg.User
}

func (p *PostgresqlDB) schema() string {
	return p.Cfg.Schema
}

func (p *PostgresqlDB) table() []string {
	return p.Cfg.Table
}

func (p *PostgresqlDB) database() string {
	return p.Db
}

// OpenDB implements the OpenDB method of the DBMS interface
func (p *PostgresqlDB) OpenDB(password string) (*sql.DB, error) {
	tableFilter := strings.Join(p.table(), ", ")
	log.WriteLog(log.MEDIUM, p.logLevel(), log.LOGFILE, "[Instance]: "+p.instance(), "[Host]: "+p.host(), "[Port]: "+strconv.Itoa(p.port()), "[Database]: "+p.database(), "[User]: "+p.user(), "[Schema]: "+p.schema(), "[Table]: "+tableFilter)
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", p.host(), p.port(), p.user(), password, p.database())
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.WriteLog(log.BASIC, p.logLevel(), log.BOTH, err.Error())
		return db, err
	}
	return db, err
}

// CloseDB implements the CloseDB method of the DBMS interface
func (p *PostgresqlDB) CloseDB(db *sql.DB) error {
	return db.Close()
}

// QueryDB implements the QueryDB method of the DBMS interface
func (p *PostgresqlDB) QueryDB(db *sql.DB) error {
	var rowSet *sql.Rows
	var tableNames []string
	var checkSum string
	var err error

	// PREPARE: Filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range p.table() {
		sqlPreparedStmt := "select TABLE_NAME from information_schema.tables where table_schema=$1 and table_name like $2"
		rowSet, err = db.Query(sqlPreparedStmt, p.schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, p.logLevel(), log.BOTH, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// Table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				log.WriteLog(log.BASIC, p.logLevel(), log.BOTH, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// Table doesn't exist in the DB schema
			log.WriteLog(log.BASIC, p.logLevel(), log.BOTH, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: Compile MD5 for all found tables
	for _, table := range tableNames {
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from information_schema.columns where table_schema=$1 and table_name=$2 order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, p.schema(), table)
		if err != nil {
			log.WriteLog(log.BASIC, p.logLevel(), log.BOTH, err.Error())
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
				log.WriteLog(log.BASIC, p.logLevel(), log.BOTH, err.Error())
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

			logColumns = append(logColumns, column)
			logColumnTypes = append(logColumnTypes, columnType)
		}
		log.WriteLog(log.FULL, p.logLevel(), log.LOGFILE, "[COLUMNS]: "+strings.Join(logColumns, ", "), "[DATATYPES]: "+strings.Join(logColumnTypes, ", "))

		// Compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table)
		sqlText := "select coalesce(md5(sum(('x' || substring(ROWHASH, 1, 8))::bit(32)::bigint)::text || sum(('x' || substring(ROWHASH, 9, 8))::bit(32)::bigint)::text ||sum(('x' || substring(ROWHASH, 17, 8))::bit(32)::bigint)::text || sum(('x' || substring(ROWHASH, 25, 8))::bit(32)::bigint)::text), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select md5(%s) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, p.schema(), table)
		log.WriteLog(log.FULL, p.logLevel(), log.LOGFILE, "[SQL]: "+sqlQueryStmt)

		err = db.QueryRow(sqlQueryStmt).Scan(&checkSum)
		if err != nil {
			log.WriteLog(log.BASIC, p.logLevel(), log.BOTH, err.Error())
			return err
		}

		result := fmt.Sprintf("%s:%s", p.instance()+"."+table, checkSum)
		log.WriteLog(log.BASIC, p.logLevel(), log.LOGFILE, "[Checksum]: "+result)
		log.WriteLog(log.BASIC, p.logLevel(), log.STDOUT, result)
	}

	return err
}
