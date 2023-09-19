package main

import (
	"database/sql"
	"fmt"

	// "md5tabsum/log"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	sLog "github.com/sabitor/simplelog"
)

var postgresqlLogPrefix string

type postgresqlDB struct {
	cfg config
	Db  string
}

func (p *postgresqlDB) logLevel() int {
	return p.cfg.loglevel
}

func (p *postgresqlDB) instance() string {
	return p.cfg.instance
}

func (p *postgresqlDB) host() string {
	return p.cfg.host
}

func (p *postgresqlDB) port() int {
	return p.cfg.port
}

func (p *postgresqlDB) user() string {
	return p.cfg.user
}

func (p *postgresqlDB) schema() string {
	return p.cfg.schema
}

func (p *postgresqlDB) table() []string {
	return p.cfg.table
}

func (p *postgresqlDB) database() string {
	return p.Db
}

// ----------------------------------------------------------------------------
func (p *postgresqlDB) openDB(password string) (*sql.DB, error) {
	postgresqlLogPrefix = "[" + p.instance() + "] -"
	tableFilter := strings.Join(p.table(), ", ")
	// log.WriteLog(log.MEDIUM, p.LogLevel(), log.LOGFILE, "[Instance]: "+p.Instance(), "[Host]: "+p.Host(), "[Port]: "+strconv.Itoa(p.Port()), "[Database]: "+p.Database(), "[User]: "+p.User(), "[Schema]: "+p.Schema(), "[Table]: "+tableFilter)
	sLog.Write(sLog.FILE, postgresqlLogPrefix, "Host:"+p.host()+",", "Port:"+strconv.Itoa(p.port())+",", "Database:"+p.database()+",", "User:"+p.user()+",", "Schema:"+p.schema()+",", "Table:"+tableFilter)
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", p.host(), p.port(), p.user(), password, p.database())
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		// log.WriteLog(log.BASIC, p.LogLevel(), log.BOTH, err.Error())
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
	var numTableRows int
	var err error
	cfgLogLevel := p.logLevel()

	// PREPARE: Filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range p.table() {
		sqlPreparedStmt := "select TABLE_NAME from information_schema.tables where table_schema=$1 and table_name like $2"
		rowSet, err = db.Query(sqlPreparedStmt, p.schema(), table)
		if err != nil {
			// log.WriteLog(log.BASIC, p.LogLevel(), log.BOTH, err.Error())
			sLog.Write(sLog.MULTI, postgresqlLogPrefix, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// Table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				// log.WriteLog(log.BASIC, p.LogLevel(), log.BOTH, err.Error())
				sLog.Write(sLog.MULTI, postgresqlLogPrefix, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// Table doesn't exist in the DB schema
			// log.WriteLog(log.BASIC, p.LogLevel(), log.BOTH, "Table "+table+" could not be found.")
			sLog.Write(sLog.MULTI, postgresqlLogPrefix, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: Compile MD5 for all found tables
	var columnNames, column, columnType string
	for _, table := range tableNames {
		// FUTURE: In case of coltype VARCHAR the max length is not yet listed. This can be done by integrating the 'character_maximum_length' column in the 'information_scheam.columns' select statement.
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from information_schema.columns where table_schema=$1 and table_name=$2 order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, p.schema(), table)
		if err != nil {
			// log.WriteLog(log.BASIC, p.LogLevel(), log.BOTH, err.Error())
			sLog.Write(sLog.MULTI, postgresqlLogPrefix, err.Error())
			return err
		}

		columnNames, column, columnType = "", "", ""
		ordinalPosition := 1

		for rowSet.Next() {
			if columnNames != "" {
				columnNames += " || "
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				// log.WriteLog(log.BASIC, p.LogLevel(), log.BOTH, err.Error())
				sLog.Write(sLog.MULTI, err.Error())
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

			sLog.Write(sLog.FILE, postgresqlLogPrefix, "Column", ordinalPosition, "of "+table+":", column, "("+columnType+")")
			ordinalPosition++
		}

		// Compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table)
		sqlText := "select count(1) NUMROWS, coalesce(md5(sum(('x' || substring(ROWHASH, 1, 8))::bit(32)::bigint)::text || sum(('x' || substring(ROWHASH, 9, 8))::bit(32)::bigint)::text ||sum(('x' || substring(ROWHASH, 17, 8))::bit(32)::bigint)::text || sum(('x' || substring(ROWHASH, 25, 8))::bit(32)::bigint)::text), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select md5(%s) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, p.schema(), table)
		// log.WriteLog(log.FULL, p.LogLevel(), log.LOGFILE, "[SQL]: "+sqlQueryStmt)
		sLog.Write(sLog.FILE, postgresqlLogPrefix, "SQL: "+sqlQueryStmt)

		err = db.QueryRow(sqlQueryStmt).Scan(&numTableRows, &checkSum)
		if err != nil {
			// log.WriteLog(log.BASIC, p.LogLevel(), log.BOTH, err.Error())
			sLog.Write(sLog.MULTI, postgresqlLogPrefix, err.Error())
			return err
		}
		sLog.Write(sLog.FILE, postgresqlLogPrefix, "Number of table rows:", numTableRows)

		// log.WriteLog(log.BASIC, p.LogLevel(), log.LOGFILE, "[Checksum]: "+result)
		// log.WriteLog(log.BASIC, p.LogLevel(), log.STDOUT, result)
		sLog.Write(sLog.STDOUT, fmt.Sprintf("%s:%s", p.instance()+"."+table, checkSum))
		sLog.Write(sLog.FILE, postgresqlLogPrefix, "Checksum: "+checkSum)

		// logWrapper(sLog.STDOUT, fmt.Sprintf("%s:%s", p.instance()+"."+table, checkSum))
		// logWrapper(sLog.FILE, postgresqlLogPrefix, "Checksum: "+checkSum)

		// sLog.ConditionalWrite(p.LogLevel()>=BASIC, sLog.FILE, postgresqlLogPrefix, "Checksum: "+checkSum)
		sLog.ConditionalWrite(logIt(cfgLogLevel, INFO), sLog.FILE, postgresqlLogPrefix, "Checksum2: "+checkSum)
	}

	return err
}
