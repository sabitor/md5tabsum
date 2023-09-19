package main

import (
	"database/sql"
)

// Database interface
type database interface {
	// openDB implements the DBMS specific open function.
	openDB(string) (*sql.DB, error)
	// closeDB implements the DBMS specific close function.
	closeDB(*sql.DB) error
	// queryDB implements any DBMS specific function using a handle returned from openDB() to work with the underlying database.
	queryDB(*sql.DB) error
}
