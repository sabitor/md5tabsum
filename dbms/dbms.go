package dbms

import (
	"database/sql"
)

// Database interface
type Database interface {
	// OpenDB implements the DBMS specific open function.
	OpenDB(string) (*sql.DB, error)
	// CloseDB implements the DBMS specific close function.
	CloseDB(*sql.DB) error
	// QueryDB implements any DBMS specific function using a handle returned from openDB() to work with the underlying database.
	QueryDB(*sql.DB) error
}

// Config defines the generic config file structure
type Config struct {
	Loglevel int
	Instance string
	Host     string
	Port     int
	User     string
	Schema   string
	Table    []string
}
