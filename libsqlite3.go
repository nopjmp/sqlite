// Copyright 2010 The Go Authors,
//           2018-2019 Kayla Thompson
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlite

/*
#cgo linux LDFLAGS: -lsqlite3
#cgo darwin LDFLAGS: -L/usr/local/opt/sqlite/lib -lsqlite3
#cgo darwin CFLAGS: -I/usr/local/opt/sqlite/include
#cgo openbsd CFLAGS: -I/usr/local/include
#cgo openbsd LDFLAGS: -L/usr/local/lib -lsqlite3
*/
import "C"
