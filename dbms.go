package main

import (
	"database/sql"
)

// DBMS interface
type dbms interface {
	// openDB implements the DBMS specific open function.
	openDB() (*sql.DB, error)
	// closeDB implements the DBMS specific close function.
	closeDB(*sql.DB) error
	// queryDB implements any DBMS specific function using a handle returned from openDB() to work with the underlying database.
	queryDB(*sql.DB) error
}

// Common config file structure
type config struct {
	instance string
	host     string
	port     int
	user     string
	schema   string
	table    []string
}

// Dbms validates the existence of a specific DBMS instance and if found, returns the instance name.
// If the DBMS instance wasn't found, the program will terminate.
func Dbms(dbmsInstance string) dbms {
	if v, ok := gDbms[dbmsInstance]; ok {
		return v
	}
	msg := "key '" + dbmsInstance + "' doesn't exist"
	panic(msg)
}
