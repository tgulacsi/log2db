/*
Copyright 2014 Tamás Gulácsi

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"database/sql"
	"errors"
	"expvar"
	"fmt"
	"log"
	"time"

	"unosoft.hu/log2db/record"
)

type Store interface {
	Insert(rec record.Record) error
	Close() error
	SaveTimes() error
	Search(after, before time.Time) (Enumerator, error)
}

type Enumerator interface {
	Next() bool
	Scan(*record.Record) error
	Close() error
}

var (
	OpenOraStore = func(params, appName string, concurrency int) (Store, error) {
		return nil, errors.New("ora store is not implemented")
	}
	OpenPgStore = func(params, appName string, concurrency int) (Store, error) {
		return nil, errors.New("pg store not implemented")
	}
	OpenQlStore = func(params, appName string) (Store, error) { return nil, errors.New("ql store is not implemented") }
)

var (
	skippedNum  = expvar.NewInt("storeSkipped")
	insertedNum = expvar.NewInt("storeInserted")
)

type bytesPool struct {
	ch chan []byte
}

func NewBytesPool(capacity int) bytesPool {
	if capacity <= 0 {
		capacity = 1
	}
	return bytesPool{ch: make(chan []byte, capacity)}
}
func (p bytesPool) Acquire(n int) []byte {
	for {
		select {
		case b := <-p.ch:
			if cap(b) >= n {
				return b[:n]
			}
		default:
			return make([]byte, n)
		}
	}
}

func (p bytesPool) Release(b []byte) {
	select {
	case p.ch <- b:
	default:
	}
}

type dbPool struct {
	*sql.DB
	insertQry string
	txPool    chan txStmt
}

type txStmt struct {
	*sql.Tx
	*sql.Stmt
	n int32
}

func (db *dbPool) getTx() (stmt *sql.Tx, release func(bool) error, err error) {
	insert, release, err := db.get()
	if err != nil {
		return nil, nil, err
	}
	return insert.Tx, release, nil
}

func (db *dbPool) getStmt() (stmt *sql.Stmt, release func(bool) error, err error) {
	insert, release, err := db.get()
	if err != nil {
		return nil, nil, err
	}
	return insert.Stmt, release, nil
}

func (db *dbPool) get() (both txStmt, release func(bool) error, err error) {
	select {
	case both = <-db.txPool:
	default:
		if both.Tx, err = db.DB.Begin(); err != nil {
			return both, nil, fmt.Errorf("error beginning transaction: %v", err)
		}
		both.Stmt, err = both.Tx.Prepare(db.insertQry)
		if err != nil {
			return both, nil, fmt.Errorf("error preparing insert: %v", err)
		}
	}
	return both, func(commit bool) error {
		if commit {
			log.Printf("COMMIT %p", both.Tx)
			if err := both.Tx.Commit(); err != nil {
				return err
			}
			return nil
		}
		select {
		case db.txPool <- both:
		default:
		}
		return nil
	}, nil
}

func (db *dbPool) Close() error {
	var (
		commitErr error
		insert    txStmt
	)
Loop:
	for {
		select {
		case insert = <-db.txPool:
			if insert.Tx != nil {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("panic with Commit(): %v", r)
						}
					}()
					commitErr = insert.Tx.Commit()
				}()
			}
		default:
			break Loop
		}
	}
	closeErr := db.DB.Close()
	if commitErr != nil {
		return commitErr
	}
	return closeErr
}
