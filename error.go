// Copyright 2018 Kayla Thompson
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

// ErrNum is the wrapper around the sqlite3 error codes
type ErrNum int

func (err ErrNum) Error() string {
	return errorString(err)
}

// Error codes taken from the sqlite3's source
const (
	ErrError      = ErrNum(1)   /* Generic error */
	ErrInternal   = ErrNum(2)   /* Internal logic error in SQLite */
	ErrPerm       = ErrNum(3)   /* Access permission denied */
	ErrAbort      = ErrNum(4)   /* Callback routine requested an abort */
	ErrBusy       = ErrNum(5)   /* The database file is locked */
	ErrLocked     = ErrNum(6)   /* A table in the database is locked */
	ErrNoMem      = ErrNum(7)   /* A malloc() failed */
	ErrReadOnly   = ErrNum(8)   /* Attempt to write a readonly database */
	ErrInterrupt  = ErrNum(9)   /* Operation terminated by sqlite3_interrupt()*/
	ErrIoErr      = ErrNum(10)  /* Some kind of disk I/O error occurred */
	ErrCorrupt    = ErrNum(11)  /* The database disk image is malformed */
	ErrNotFound   = ErrNum(12)  /* Unknown opcode in sqlite3_file_control() */
	ErrFull       = ErrNum(13)  /* Insertion failed because database is full */
	ErrCantOpen   = ErrNum(14)  /* Unable to open the database file */
	ErrProtocol   = ErrNum(15)  /* Database lock protocol error */
	ErrEmpty      = ErrNum(16)  /* Internal use only */
	ErrSchema     = ErrNum(17)  /* The database schema changed */
	ErrTooBig     = ErrNum(18)  /* String or BLOB exceeds size limit */
	ErrConstraint = ErrNum(19)  /* Abort due to constraint violation */
	ErrMismatch   = ErrNum(20)  /* Data type mismatch */
	ErrMisuse     = ErrNum(21)  /* Library used incorrectly */
	ErrNoLFS      = ErrNum(22)  /* Uses OS features not supported on host */
	ErrAuth       = ErrNum(23)  /* Authorization denied */
	ErrFormat     = ErrNum(24)  /* Not used */
	ErrRange      = ErrNum(25)  /* 2nd parameter to sqlite3_bind out of range */
	ErrNotADB     = ErrNum(26)  /* File opened that is not a database file */
	ErrNotice     = ErrNum(27)  /* Notifications from sqlite3_log() */
	ErrWarning    = ErrNum(28)  /* Warnings from sqlite3_log() */
	ErrRow        = ErrNum(100) /* sqlite3_step() has another row ready */
	ErrDone       = ErrNum(101) /* sqlite3_step() has finished executing */
)
