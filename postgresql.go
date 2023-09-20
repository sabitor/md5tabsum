package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/sabitor/simplelog"
)

var postgresqlLogPrefix string

type postgresqlDB struct {
	cfg config
	Db  string
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
	simplelog.ConditionalWrite(condition(pr.logLevel, DEBUG), simplelog.FILE, postgresqlLogPrefix, "Host:"+p.host()+",", "Port:"+strconv.Itoa(p.port())+",", "Database:"+p.database()+",", "User:"+p.user()+",", "Schema:"+p.schema()+",", "Table:"+tableFilter)
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", p.host(), p.port(), p.user(), password, p.database())
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		simplelog.Write(simplelog.MULTI, err.Error())
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

	// PREPARE: filter for all existing DB tables based on the configured table parameter (the tables parameter can include placeholders, e.g. %)
	for _, table := range p.table() {
		sqlPreparedStmt := "select TABLE_NAME from information_schema.tables where table_schema=$1 and table_name like $2"
		rowSet, err = db.Query(sqlPreparedStmt, p.schema(), table)
		if err != nil {
			simplelog.Write(simplelog.MULTI, postgresqlLogPrefix, err.Error())
			return err
		}
		foundTable := ""
		for rowSet.Next() {
			// table exists in DB schema
			err := rowSet.Scan(&foundTable)
			if err != nil {
				simplelog.Write(simplelog.MULTI, postgresqlLogPrefix, err.Error())
				return err
			}
			tableNames = append(tableNames, foundTable)
		}
		if foundTable == "" {
			// table doesn't exist in the DB schema
			simplelog.Write(simplelog.MULTI, postgresqlLogPrefix, "Table "+table+" could not be found.")
		}
	}

	// EXECUTE: compile MD5 for all found tables
	for _, table := range tableNames {
		// FUTURE: In case of coltype VARCHAR the max length is not yet listed. This can be done by integrating the 'character_maximum_length' column in the 'information_scheam.columns' select statement.
		sqlPreparedStmt := "select COLUMN_NAME, DATA_TYPE from information_schema.columns where table_schema=$1 and table_name=$2 order by ORDINAL_POSITION asc"
		rowSet, err = db.Query(sqlPreparedStmt, p.schema(), table)
		if err != nil {
			simplelog.Write(simplelog.MULTI, postgresqlLogPrefix, err.Error())
			return err
		}

		columnNames, column, columnType := "", "", ""
		ordinalPosition := 1

		for rowSet.Next() {
			if columnNames != "" {
				columnNames += " || "
			}
			err := rowSet.Scan(&column, &columnType)
			if err != nil {
				simplelog.Write(simplelog.MULTI, err.Error())
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

			simplelog.ConditionalWrite(condition(pr.logLevel, TRACE), simplelog.FILE, postgresqlLogPrefix, "Column", ordinalPosition, "of "+table+":", column, "("+columnType+")")
			ordinalPosition++
		}

		// Compile checksum (d41d8cd98f00b204e9800998ecf8427e is the default result for an empty table) by using the following SQL:
		//   select count(1) NUMROWS,
		//          coalesce(md5(sum(('x' || substring(ROWHASH, 1, 8))::bit(32)::bigint)::text ||
		//                       sum(('x' || substring(ROWHASH, 9, 8))::bit(32)::bigint)::text ||
		//                       sum(('x' || substring(ROWHASH, 17, 8))::bit(32)::bigint)::text ||
		//                       sum(('x' || substring(ROWHASH, 25, 8))::bit(32)::bigint)::text),
		//                   'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM
		//   from (select md5(%s) ROWHASH from %s.%s) t
		sqlText := "select count(1) NUMROWS, coalesce(md5(sum(('x' || substring(ROWHASH, 1, 8))::bit(32)::bigint)::text || sum(('x' || substring(ROWHASH, 9, 8))::bit(32)::bigint)::text ||sum(('x' || substring(ROWHASH, 17, 8))::bit(32)::bigint)::text || sum(('x' || substring(ROWHASH, 25, 8))::bit(32)::bigint)::text), 'd41d8cd98f00b204e9800998ecf8427e') CHECKSUM from (select md5(%s) ROWHASH from %s.%s) t"
		sqlQueryStmt := fmt.Sprintf(sqlText, columnNames, p.schema(), table)
		simplelog.ConditionalWrite(condition(pr.logLevel, TRACE), simplelog.FILE, postgresqlLogPrefix, "SQL: "+sqlQueryStmt)

		err = db.QueryRow(sqlQueryStmt).Scan(&numTableRows, &checkSum)
		if err != nil {
			simplelog.Write(simplelog.MULTI, postgresqlLogPrefix, err.Error())
			return err
		}
		simplelog.ConditionalWrite(condition(pr.logLevel, DEBUG), simplelog.FILE, postgresqlLogPrefix, "Number of table rows:", numTableRows)

		simplelog.Write(simplelog.STDOUT, fmt.Sprintf("%s:%s", p.instance()+"."+table, checkSum))
		simplelog.Write(simplelog.FILE, postgresqlLogPrefix, "Checksum: "+checkSum)
	}

	return err
}
