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
	"sync"
	"time"

	"unosoft.hu/log2db/parsers"

	_ "github.com/cznic/ql/driver"
)

type Store interface {
	Insert(rec parsers.Record) error
	Close() error
	SaveTime() error
}

type qlStore struct {
	*sql.DB
	appName   string
	n         int
	last, act time.Time
	insertQry string
	insert    *sql.Stmt
	tx        *sql.Tx
	sync.Mutex
}

func (db *qlStore) snapshot() (*sql.Stmt, error) {
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

func (db *qlStore) Close() error {
	db.Lock()
	defer db.Unlock()
	var commitErr error
	if db.tx != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("panic with Commit(): %v", r)
				}
			}()
			commitErr = db.tx.Commit()
			db.tx = nil
		}()
	}
	log.Println("transaction")
	if err := db.SaveTime(); err != nil {
		log.Printf("error saving time: %v", err)
	}
	closeErr := db.DB.Close()
	if commitErr != nil {
		return commitErr
	}
	return closeErr
}

func (db *qlStore) SaveTime() error {
	tx := db.tx
	var err error
	if tx == nil {
		tx, err = db.DB.Begin()
		if err != nil {
			log.Printf("error beginning transaction: %v", err)
		}
	}
	log.Println("Deleting T_last")
	if _, err = tx.Exec("DELETE FROM T_last WHERE F_app == $1", db.appName); err != nil {
		log.Printf("error DELETing: %v", err)
		tx.Rollback()
	}
	if err = tx.Commit(); err != nil {
		log.Printf("commit error: %v", err)
	}
	log.Println("deleted.")
	if tx, err = db.DB.Begin(); err != nil {
		log.Printf("error beginning transaction: %v", err)
	} else {
		log.Printf("inserting last time for %s: %s", db.appName, db.act)
		if _, err = tx.Exec("INSERT INTO T_last (F_app, F_last) VALUES ($1, $2)",
			db.appName, db.act,
		); err != nil {
			log.Printf("error setting last: %v", err)
			tx.Commit()
		}
		log.Println("INSERTed")
	}
	db.tx = nil
	return nil
}

func (db *qlStore) Insert(rec parsers.Record) error {
	if db.last.After(rec.When) {
		// SKIP
		return nil
	}
	if db.act.Before(rec.When) {
		db.act = rec.When
	}
	if db.insert == nil || db.n%1000 == 0 {
		if _, err := db.snapshot(); err != nil {
			return err
		}
	}
	if _, err := db.insert.Exec(rec.App, rec.Type, rec.When, rec.SessionID,
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
	if err != nil {
		return nil, fmt.Errorf("error opening ql db: %v", err)
	}
	empty := true
	tx, e := db.Begin()
	if e != nil {
		return nil, fmt.Errorf("error beginning transaction: %v", e)
	}
	if _, err = tx.Exec(`CREATE TABLE IF NOT EXISTS T_last (F_app string, F_last time)`); err != nil {
		return nil, fmt.Errorf("error creating T_last: %v", err)
	}
	tx.Commit()
	row := db.QueryRow(`SELECT formatTime(F_last, "`+time.RFC3339+`")
        FROM T_last WHERE F_app == $1`, appName)
	var lt sql.NullString
	if err = row.Scan(&lt); err == nil {
		if lt.Valid {
			if lastTime, err = time.Parse(time.RFC3339, lt.String); err != nil {
				log.Fatalf("error parsing %s: %v", lt, err)
			}
			empty = false
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
	return &qlStore{
		DB:      db,
		last:    lastTime,
		appName: appName,
		insertQry: `
INSERT INTO T_log (F_app, F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc, F_id)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
	}, nil
}
