package dbms

import (
	"database/sql"
)

// Database interface
type Database interface {
	// openDB implements the DBMS specific open function.
	openDB() (*sql.DB, error)
	// closeDB implements the DBMS specific close function.
	closeDB(*sql.DB) error
	// queryDB implements any DBMS specific function using a handle returned from openDB() to work with the underlying database.
	queryDB(*sql.DB) error
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
