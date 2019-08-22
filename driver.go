// Copyright 2010 The Go Authors,
//           2018-2019 Kayla Thompson
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

/*
extern size_t _GoStringLen(_GoString_ s);
extern const char *_GoStringPtr(_GoString_ s);
#include <sqlite3.h>
#include <stdlib.h>
#include <stddef.h>

typedef int (*busy_callback)(void*,int);

extern int busyHandler(void*, int);

static void registerBusyHandler(sqlite3 *db) {
	sqlite3_busy_handler(db, (busy_callback)busyHandler, (void*)db);
}

static int _bind_text(sqlite3_stmt *stmt, int n, _GoString_ s) {
	const char *p = _GoStringPtr(s);
	if (p == NULL)
		p = "";
	return sqlite3_bind_text(stmt, n, p, _GoStringLen(s), SQLITE_TRANSIENT);
}

static int _bind_blob(sqlite3_stmt *stmt, int n, const void *p, int np) {
	if (p == NULL)
		p = "";
	return sqlite3_bind_blob(stmt, n, p, np, SQLITE_TRANSIENT);
}

static int _exec_step(sqlite3 *db, sqlite3_stmt *stmt, int64_t *rowid, int64_t *changes) {
	int rv = sqlite3_step(stmt);
	*rowid = sqlite3_last_insert_rowid(db);
	*changes = sqlite3_changes(db);
	return rv;
}
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"
	"unsafe"
)

func init() {
	sql.Register("sqlite3", impl{})
}

func errorString(code ErrNum) string {
	return C.GoString(C.sqlite3_errstr(C.int(code)))
}

var (
	busyTimeout = time.Duration(30) * time.Second
	busyDelays  = []int{1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100}
	busyTotals  = []int{0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228}
	busySize    = len(busyDelays)
)

// busyHandler is similar to busy_timeout, but uses time.Sleep to yield execution to another goroutine
// TODO: wait for a condition or channel to signal us to continue with a long timeout

//export busyHandler
func busyHandler(ptr unsafe.Pointer, c C.int) C.int {
	count := int(c)

	var delayInt, priorInt int
	if count < busySize {
		delayInt = busyDelays[count]
		priorInt = busyTotals[count]
	} else {
		delayInt = busyDelays[busySize-1]
		priorInt = busyTotals[busySize-1] + delayInt*(count-busySize)
	}
	delay := time.Duration(delayInt) * time.Microsecond
	prior := time.Duration(priorInt) * time.Microsecond

	if prior+delay > busyTimeout {
		delay = busyTimeout - prior
		if delay < 0 {
			return 0
		}
	}

	// yield to other goroutines
	time.Sleep(delay)
	return 1
}

var timeFormat = []string{
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

type impl struct{}

func (impl) Open(dsn string) (driver.Conn, error) {
	if C.sqlite3_threadsafe() == 0 {
		return nil, errors.New("sqlite library has to be compiled as thread-safe")
	}

	var db *C.sqlite3
	// TODO: parse this
	namestr := C.CString(dsn)

	rv := C.sqlite3_open_v2(namestr, &db,
		C.SQLITE_OPEN_FULLMUTEX|C.SQLITE_OPEN_READWRITE|C.SQLITE_OPEN_CREATE,
		nil)
	C.free(unsafe.Pointer(namestr))

	if rv != C.SQLITE_OK {
		return nil, ErrNum(rv)
	}

	if db == nil {
		return nil, errors.New("sqlite returned without returning a database")
	}

	C.registerBusyHandler(db)

	c := &conn{db: db}
	runtime.SetFinalizer(c, (*conn).Close)
	return c, nil
}

type conn struct {
	db     *C.sqlite3
	closed atomicBool
	tx     atomicBool
}

func lastError(db *C.sqlite3) error {
	rv := C.sqlite3_errcode(db)
	if rv == C.SQLITE_OK {
		return nil
	}

	return ErrNum(rv)
}

func interrupt(ctx context.Context, db *C.sqlite3, done chan bool) {
	select {
	case <-done: // do nothing
	case <-ctx.Done():
		// TODO: we should provide support for running a function after the interrupt
		C.sqlite3_interrupt(db)
	}
}

func (c *conn) Ping() error {
	if c.closed.IsSet() {
		return errors.New("connection closed")
	}
	return nil
}

func (c *conn) lastError() error {
	return lastError(c.db)
}

// Exec for Execer interface
func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		list[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return c.ExecContext(context.Background(), query, list)
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	cursor := 0
	for {
		s, err := c.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}

		need := s.NumInput()
		if len(args) < need {
			s.Close()
			return nil, fmt.Errorf("not enough args (%d / %d)", need, len(args))
		}

		for i := 0; i < need; i++ {
			args[i].Ordinal -= cursor
		}

		res, err := s.(*stmt).ExecContext(ctx, args[:need])
		if err != nil && err != driver.ErrSkip {
			s.Close()
			return nil, err
		}

		args = args[need:]
		cursor += need

		// grab the tail and check if we can finish, Execer support
		tail := s.(*stmt).tail
		s.Close()
		if tail == "" {
			return res, nil
		}

		query = tail
	}
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if c.closed.IsSet() {
		return nil, io.EOF
	}

	querystr := C.CString(query)
	defer C.free(unsafe.Pointer(querystr))

	var ss *C.sqlite3_stmt
	var ctail *C.char // used for Execer
	rv := C.sqlite3_prepare_v2(c.db, querystr, C.int(-1), &ss, &ctail)

	if rv != C.SQLITE_OK {
		return nil, c.lastError()
	}

	var tail string
	if ctail != nil && *ctail != '\x00' {
		tail = strings.TrimSpace(C.GoString(ctail))
	}

	s := &stmt{c: c, ss: ss, tail: tail, query: query}
	runtime.SetFinalizer(s, (*stmt).Close)
	return s, nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *conn) Close() error {
	if c.closed.IsSet() {
		return io.EOF
	}
	// we don't care if sqlite3_close fails, just leak if it breaks
	c.closed.Set(true)

	rv := C.sqlite3_close(c.db)
	c.db = nil
	runtime.SetFinalizer(c, nil)
	if rv != 0 {
		return c.lastError()
	}
	return nil
}

func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// TODO: use savepoints
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if _, err := c.ExecContext(ctx, "BEGIN", nil); err != nil {
		return nil, err
	}
	c.tx.Set(true)

	// context cancellation support
	done := make(chan bool)

	// TODO: we need to attempt to rollback here, if we haven't done COMMIT
	if ctx.Done() != nil {
		go interrupt(ctx, c.db, done)
	}

	return &tx{c, ctx, done}, nil
}

type tx struct {
	c    *conn
	ctx  context.Context
	done chan bool
}

func (t *tx) Commit() error {
	if t.c == nil || !t.c.tx.IsSet() {
		return errors.New("extra commit")
	}
	t.c.tx.Set(false)
	_, err := t.c.ExecContext(t.ctx, "COMMIT", nil)
	if err != nil && err == ErrBusy {
		t.c.ExecContext(t.ctx, "ROLLBACK", nil)
	}
	close(t.done)
	t.c = nil
	return err
}

func (t *tx) Rollback() error {
	if t.c == nil || !t.c.tx.IsSet() {
		return errors.New("extra rollback")
	}
	t.c.tx.Set(false)
	_, err := t.c.ExecContext(t.ctx, "ROLLBACK", nil)
	close(t.done)
	t.c = nil
	return err
}

type stmt struct {
	c      *conn
	ss     *C.sqlite3_stmt
	tail   string
	query  string
	err    error
	args   string
	closed atomicBool
	rows   atomicBool
}

func (s *stmt) Close() error {
	if s.closed.IsSet() {
		return io.EOF
	}
	// we don't care if sqlite3_close fails, just leak if it breaks
	s.closed.Set(true)

	rv := C.sqlite3_finalize(s.ss)
	s.ss = nil
	runtime.SetFinalizer(s, nil)
	if rv != C.SQLITE_OK {
		return ErrNum(rv)
	}
	return nil
}

func (s *stmt) NumInput() int {
	if s.closed.IsSet() {
		return -1
	}
	return int(C.sqlite3_bind_parameter_count(s.ss))
}

func (s *stmt) bind(args []driver.NamedValue) error {
	rv := C.sqlite3_reset(s.ss)
	if rv != C.SQLITE_ROW && rv != C.SQLITE_OK && rv != C.SQLITE_DONE {
		return ErrNum(rv)
	}

	count := int(C.sqlite3_bind_parameter_count(s.ss))
	if count != len(args) {
		return fmt.Errorf("incorrect arguments: got %d need %d", len(args), count)
	}

	for i, v := range args {
		if v.Name != "" {
			cname := C.CString(":" + v.Name)
			args[i].Ordinal = int(C.sqlite3_bind_parameter_index(s.ss, cname))
			C.free(unsafe.Pointer(cname))
		}
	}

	for _, arg := range args {
		n := C.int(arg.Ordinal)
		switch v := arg.Value.(type) {
		case nil:
			rv = C.sqlite3_bind_null(s.ss, n)
		case int64:
			rv = C.sqlite3_bind_int64(s.ss, n, C.sqlite3_int64(v))
		case bool:
			if v {
				rv = C.sqlite3_bind_int(s.ss, n, C.int(1))
			} else {

				rv = C.sqlite3_bind_int(s.ss, n, C.int(0))
			}
		case float64:
			rv = C.sqlite3_bind_double(s.ss, n, C.double(v))
		case []byte:
			var p unsafe.Pointer
			ln := len(v)
			if ln > 0 {
				p = unsafe.Pointer(&v[0])
			}
			rv = C._bind_blob(s.ss, n, p, C.int(ln))
		case time.Time:
			str := v.UTC().Format(timeFormat[0])
			rv = C._bind_text(s.ss, n, str)
		case string:
			rv = C._bind_text(s.ss, n, v)
		}
		if rv != C.SQLITE_OK {
			return s.c.lastError()
		}
	}
	return nil
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		list[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return s.ExecContext(context.Background(), list)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.closed.IsSet() {
		return nil, io.EOF
	}

	if s.rows.IsSet() {
		return nil, errors.New("exec with active rows")
	}

	err := s.bind(args)
	if err != nil {
		return nil, err
	}

	// context cancellation support
	if ctx.Done() != nil {
		done := make(chan bool)
		defer close(done)
		go interrupt(ctx, s.c.db, done)
	}

	var rowid, changes C.int64_t
	rv := C._exec_step(s.c.db, s.ss, &rowid, &changes)
	if rv != C.SQLITE_ROW && rv != C.SQLITE_OK && rv != C.SQLITE_DONE {
		err = s.c.lastError()
		C.sqlite3_reset(s.ss)
		C.sqlite3_clear_bindings(s.ss)
		return nil, err
	}

	return &result{int64(rowid), int64(changes)}, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	list := make([]driver.NamedValue, len(args))
	for i, v := range args {
		list[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return s.QueryContext(context.Background(), list)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.closed.IsSet() {
		return nil, io.EOF
	}

	if s.rows.IsSet() {
		return nil, errors.New("query with active rows")
	}

	err := s.bind(args)
	if err != nil {
		return nil, err
	}

	s.rows.Set(true)
	done := make(chan bool)
	rows := &rows{s, int64(C.sqlite3_column_count(s.ss)), nil, nil, done}

	// context cancellation support
	if ctx.Done() != nil {
		go interrupt(ctx, s.c.db, done)
	}

	return rows, nil
}

type rows struct {
	s        *stmt
	colcount int64
	colnames []string
	coltypes []string
	done     chan bool
}

func (r *rows) Columns() []string {
	if r.s == nil {
		return nil
	}

	if r.colnames == nil {
		r.colnames = make([]string, r.colcount)
		for i := range r.colnames {
			r.colnames[i] = C.GoString(C.sqlite3_column_name(r.s.ss, C.int(i)))
		}
	}

	return r.colnames
}

func (r *rows) Next(dst []driver.Value) error {
	if r.s == nil || r.s.closed.IsSet() {
		return io.EOF
	}

	rv := C.sqlite3_step(r.s.ss)
	if rv == C.SQLITE_DONE {
		return io.EOF
	}

	if rv != C.SQLITE_ROW {
		rv = C.sqlite3_reset(r.s.ss)
		if rv != C.SQLITE_OK {
			return r.s.c.lastError()
		}
	}

	if r.coltypes == nil {
		r.coltypes = make([]string, r.colcount)
		for i := range r.coltypes {
			r.coltypes[i] = strings.ToLower(C.GoString(C.sqlite3_column_decltype(r.s.ss, C.int(i))))
		}
	}

	for i := range dst {
		n := C.int(i)
		switch C.sqlite3_column_type(r.s.ss, n) {
		case C.SQLITE_INTEGER:
			val := int64(C.sqlite3_column_int64(r.s.ss, n))
			switch r.coltypes[i] {
			case "timestamp", "datetime":
				dst[i] = time.Unix(val, 0).UTC()
			case "boolean":
				dst[i] = val > 0
			default:
				dst[i] = val
			}

		case C.SQLITE_FLOAT:
			dst[i] = float64(C.sqlite3_column_double(r.s.ss, n))

		case C.SQLITE_BLOB:
			p := C.sqlite3_column_blob(r.s.ss, n)
			if p == nil {
				dst[i] = nil
				continue
			}
			c := C.sqlite3_column_bytes(r.s.ss, n)
			dst[i] = C.GoBytes(p, c)

		case C.SQLITE_TEXT:
			c := C.sqlite3_column_bytes(r.s.ss, n)
			s := C.GoStringN((*C.char)(unsafe.Pointer(C.sqlite3_column_text(r.s.ss, n))), c)

			switch r.coltypes[i] {
			case "timestamp", "datetime":
				dst[i] = time.Time{}
				for _, f := range timeFormat {
					if t, err := time.Parse(f, s); err == nil {
						dst[i] = t
					}
				}

			default:
				dst[i] = s
			}
		}
	}
	return nil
}

func (r *rows) Close() error {
	if r.s == nil {
		return errors.New("Close of closed Rows")
	}
	r.s.rows.Set(false)
	close(r.done)
	r.s = nil
	return nil
}

type result struct {
	rowid   int64
	changes int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.rowid, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.changes, nil
}
