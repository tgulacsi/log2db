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
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"unosoft.hu/log2db/parsers"

	_ "github.com/cznic/ql/driver"
)

type Store interface {
	Insert(appName string, rec parsers.Record) error
	Close() error
}

type dbStore struct {
	*sql.DB
	n         int
	lastTime  time.Time
	insertQry string
	insert    *sql.Stmt
	tx        *sql.Tx
}

func (db *dbStore) snapshot() (*sql.Stmt, error) {
	if db.tx != nil {
		db.tx.Commit()
	}
	var err error
	if db.tx, err = db.Begin(); err != nil {
		return nil, fmt.Errorf("error beginning transaction: %v", err)
	}
	db.insert, err = db.tx.Prepare(db.insertQry)
	if err != nil {
		return nil, fmt.Errorf("error preparing insert: %v", err)
	}
	return db.insert, nil
}

func (db *dbStore) Close() error {
	var commitErr error
	if db.tx != nil {
		commitErr = db.tx.Commit()
	}
	closeErr := db.DB.Close()
	if commitErr != nil {
		return commitErr
	}
	return closeErr
}

func (db *dbStore) Insert(appName string, rec parsers.Record) error {
	if db.lastTime.After(rec.When) {
		// SKIP
		return nil
	}
	if db.insert == nil || db.n%1000 == 0 {
		if _, err := db.snapshot(); err != nil {
			return err
		}
	}
	if _, err := db.insert.Exec(appName, rec.Type, rec.When, rec.SessionID,
		rec.Text, rec.EventID, rec.Command, rec.Background, rec.RC,
		base64.StdEncoding.EncodeToString(rec.ID()),
	); err != nil {
		return err
	}
	return nil
}

func OpenQlStore(params, appName string) (Store, error) {
	var lastTime time.Time
	db, err := sql.Open("ql", params)
	defer func() {
		log.Printf("sql.Open(ql, %q): %s, %s, %v", params, db, lastTime, err)
	}()
	if err != nil {
		return nil, fmt.Errorf("error opening ql db: %v", err)
	}
	empty := true
	rows, err := db.Query(`SELECT formatTime(F_date, "`+time.RFC3339+`")
        FROM T_log WHERE F_app == $1`, appName)
	if err == nil {
		var lt sql.NullString
		var t time.Time
		for rows.Next() {
			if err = rows.Scan(&lt); err == nil {
				if lt.Valid {
					if t, err = time.Parse(time.RFC3339, lt.String); err != nil {
						log.Fatalf("error parsing %s: %v", lt, err)
					}
					empty = false
					if t.After(lastTime) {
						lastTime = t
					}
				}
			}
		}
	}
	if empty {
		tx, e := db.Begin()
		if e != nil {
			return nil, fmt.Errorf("error beginning transaction: %v", e)
		}
		if _, err = tx.Exec(`CREATE TABLE IF NOT EXISTS T_log (
                F_app string,
                F_type int64,
                F_date time,
                F_sid int64,
                F_text string,
                F_evid int64,
                F_cmd string,
                F_bg bool,
                F_rc int64,
                F_id string
            )`,
		); err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("error creating table T_log: %v", err)
		}
		tx.Commit()
	}
	return &dbStore{
		DB:       db,
		lastTime: lastTime,
		insertQry: `
INSERT INTO T_log (F_app, F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc, F_id)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
	}, nil
}
