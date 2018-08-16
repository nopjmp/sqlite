// Copyright 2010 The Go Authors,
//           2018 Kayla Thompson
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

/*
#cgo LDFLAGS: -lsqlite3

#include <sqlite3.h>
#include <stdlib.h>

typedef int (*busy_callback)(void*,int);

extern int busyHandler(void*, int);

static void registerBusyHandler(sqlite3 *db) {
	sqlite3_busy_handler(db, (busy_callback)busyHandler, (void*)db);
}

static int _bind_text(sqlite3_stmt *stmt, int n, char *p, int np) {
	return sqlite3_bind_text(stmt, n, p, np, SQLITE_TRANSIENT);
}

static int _bind_blob(sqlite3_stmt *stmt, int n, char *p, int np) {
	return sqlite3_bind_blob(stmt, n, p, np, SQLITE_TRANSIENT);
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
	busyTimeout = 30000
	busyDelays  = []int{1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100}
	busyTotals  = []int{0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228}
	busySize    = len(busyDelays)
)

// busyHandler is similar to busy_timeout, but uses time.Sleep to yield execution to another goroutine
// TODO: wait for a condition or channel to signal us to continue with a long timeout

//export busyHandler
func busyHandler(ptr unsafe.Pointer, c C.int) C.int {
	count := int(c)

	var delay, prior int
	if count < busySize {
		delay = busyDelays[count]
		prior = busyTotals[count]
	} else {
		delay = busyDelays[busySize-1]
		prior = busyTotals[busySize-1] + delay*(count-busySize)
	}

	if prior+delay > busyTimeout {
		delay = busyTimeout - prior
		if delay < 0 {
			return 0
		}
	}
	// yield to other goroutines
	time.Sleep(time.Duration(delay) * time.Millisecond)
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

	conn := &conn{db: db}
	return conn, nil
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

	var ss *C.sqlite3_stmt
	var ctail *C.char // used for Execer
	rv := C.sqlite3_prepare_v2(c.db, querystr, C.int(len(query)+1), &ss, &ctail)

	C.free(unsafe.Pointer(querystr))

	if rv != C.SQLITE_OK {
		return nil, c.lastError()
	}

	var tail string
	if ctail != nil && *ctail != '\x00' {
		tail = strings.TrimSpace(C.GoString(ctail))
	}

	s := &stmt{c: c, ss: ss, tail: tail, query: query}
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
	defer close(done)
	go func(db *C.sqlite3) {
		select {
		case <-done:
		case <-ctx.Done():
			// TODO: we need to attempt to rollback here, if we haven't done COMMIT
			C.sqlite3_interrupt(db)
		}
	}(c.db)

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
	c        *conn
	ss       *C.sqlite3_stmt
	tail     string
	query    string
	err      error
	args     string
	closed   atomicBool
	rows     atomicBool
	colnames []string
	coltypes []string
}

func (s *stmt) Close() error {
	if s.closed.IsSet() {
		return io.EOF
	}
	// we don't care if sqlite3_close fails, just leak if it breaks
	s.closed.Set(true)

	rv := C.sqlite3_finalize(s.ss)
	s.ss = nil
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
			var p *byte
			if len(v) > 0 {
				p = &v[0]
			}
			rv = C._bind_blob(s.ss, n, (*C.char)(unsafe.Pointer(p)), C.int(len(v)))
		case time.Time:
			str := v.UTC().Format(timeFormat[0])
			cstr := C.CString(str)
			rv = C._bind_text(s.ss, n, cstr, C.int(len(str)))
			C.free(unsafe.Pointer(cstr))
		case string:
			cstr := C.CString(v)
			rv = C._bind_text(s.ss, n, cstr, C.int(len(v)))
			C.free(unsafe.Pointer(cstr))
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
	done := make(chan bool)
	defer close(done)
	go func(db *C.sqlite3) {
		select {
		case <-done:
		case <-ctx.Done():
			C.sqlite3_interrupt(db)
		}
	}(s.c.db)

	rv := C.sqlite3_step(s.ss)
	if rv != C.SQLITE_ROW && rv != C.SQLITE_OK && rv != C.SQLITE_DONE {
		err = s.c.lastError()
		C.sqlite3_reset(s.ss)
		C.sqlite3_clear_bindings(s.ss)
		return nil, err
	}

	id := int64(C.sqlite3_last_insert_rowid(s.c.db))
	rows := int64(C.sqlite3_changes(s.c.db))
	return &result{id, rows}, nil
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
	if s.colnames == nil {
		n := int64(C.sqlite3_column_count(s.ss))
		s.colnames = make([]string, n)
		s.coltypes = make([]string, n)
		for i := range s.colnames {
			s.colnames[i] = C.GoString(C.sqlite3_column_name(s.ss, C.int(i)))
			s.coltypes[i] = strings.ToLower(C.GoString(C.sqlite3_column_decltype(s.ss, C.int(i))))
		}
	}
	done := make(chan bool)
	rows := &rows{s, done}

	// context cancellation support
	go func(db *C.sqlite3) {
		select {
		case <-rows.done:
		case <-ctx.Done():
			C.sqlite3_interrupt(db)
			rows.Close()
		}
	}(s.c.db)

	return rows, nil
}

type rows struct {
	s    *stmt
	done chan bool
}

func (r *rows) Columns() []string {
	if r.s == nil {
		return nil
	}
	return r.s.colnames
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

	for i := range dst {
		n := C.int(i)
		switch ctype := C.sqlite3_column_type(r.s.ss, n); ctype {
		default:
			return fmt.Errorf("unexpected sqlite3 column type %d", ctype)

		case C.SQLITE_INTEGER:
			val := int64(C.sqlite3_column_int64(r.s.ss, n))
			switch r.s.coltypes[i] {
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
			c := int(C.sqlite3_column_bytes(r.s.ss, n))
			s := make([]byte, c)
			copy(s[:], (*[1 << 30]byte)(unsafe.Pointer(p))[:n])
			dst[i] = s

		case C.SQLITE_TEXT:
			c := C.sqlite3_column_bytes(r.s.ss, n)
			s := C.GoStringN((*C.char)(unsafe.Pointer(C.sqlite3_column_text(r.s.ss, n))), c)

			switch r.s.coltypes[i] {
			case "timestamp", "datetime":
				dst[i] = time.Time{}
				for _, f := range timeFormat {
					if t, err := time.Parse(f, s); err == nil {
						dst[i] = t
					}
				}

			default:
				dst[i] = []byte(s)
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
	r.s = nil
	close(r.done)
	return nil
}

type result struct {
	id   int64
	rows int64
}

func (r *result) LastInsertId() (int64, error) {
	return r.id, nil
}

func (r *result) RowsAffected() (int64, error) {
	return r.rows, nil
}
