package constant

const (
	// -- Common --
	VERSION     = "1.1.1"
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
