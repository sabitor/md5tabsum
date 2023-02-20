package constant

const (
	// -- Common --
	VERSION     = "1.1.0"
	EXECUTABLE  = "md5tabsum"
	EMPTYSTRING = ""
	OK          = 0
	ERROR       = 2

	// -- Logging --
	LOGFILE = 1
	STDOUT  = 2
	BOTH    = 3

	// -- Password store --
	// The cipher key has to be either 16, 24 or 32 bytes. Change it accordingly!
	CIPHERKEY = "abcdefghijklmnopqrstuvwxyz012345"
)

var (
	// map to store instances and their assigned log level
	gInstanceLogLevel = make(map[string]int)
	// -- DBMS interface --
	// contains all supported DBMS
	gSupportedDbms = []string{"exasol", "mysql", "mssql", "oracle", "postgresql"}
	// Dbms contains the DBMS instance configuration for any active config file section
	// mDbms[Key -> DBMS instance name : Value -> DBMS instance config]
	// Example: mDbms["exasol.instance1":exasolDB DBMS interface]
	gDbms = make(map[string]dbms.Database)
)
