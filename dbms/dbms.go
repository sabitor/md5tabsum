package dbms

import (
	"database/sql"
)

// Database interface
type Database interface {
	// openDB implements the DBMS specific open function.
	OpenDB(string) (*sql.DB, error)
	// closeDB implements the DBMS specific close function.
	CloseDB(*sql.DB) error
	// queryDB implements any DBMS specific function using a handle returned from openDB() to work with the underlying database.
	QueryDB(*sql.DB) error
}

// Common config file structure
type Config struct {
	Instance string
	Host     string
	Port     int
	User     string
	Schema   string
	Table    []string
}
