// +build ql

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

	"unosoft.hu/log2db/record"

	_ "github.com/cznic/ql/driver"
)

func init() {
	OpenQlStore = openQlStore
}

type qlStore struct {
	*sql.DB
	appName          string
	n                int
	first, last, act time.Time
	insertQry        string
	insert           *sql.Stmt
	tx               *sql.Tx
	sync.Mutex
}

func (db *qlStore) snapshot() (*sql.Stmt, error) {
	if db.tx != nil {
		db.tx.Commit()
	}
	if db.insert != nil {
		log.Printf("closing %s", db.insert)
		db.insert.Close()
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
	if err := db.SaveTimes(); err != nil {
		log.Printf("error saving time: %v", err)
	}
	closeErr := db.DB.Close()
	if commitErr != nil {
		return commitErr
	}
	return closeErr
}

func (db *qlStore) SaveTimes() error {
	tx := db.tx
	var err error
	if tx == nil {
		tx, err = db.DB.Begin()
		if err != nil {
			log.Printf("error beginning transaction: %v", err)
		}
	}
	log.Println("Deleting T_times")
	if _, err = tx.Exec("DELETE FROM T_times WHERE F_app == $1", db.appName); err != nil {
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
		if _, err = tx.Exec("INSERT INTO T_times (F_app, F_first, F_last) VALUES ($1, $2, $3)",
			db.appName, db.first, db.act,
		); err != nil {
			log.Printf("error setting times: %v", err)
			tx.Commit()
		}
		log.Println("INSERTed")
	}
	db.tx = nil
	return nil
}

func (db *qlStore) Insert(rec record.Record) error {
	log.Printf("Insert(%s)", rec.When)
	if db.first.IsZero() {
		db.first = rec.When
	}
	if db.last.After(rec.When) {
		// SKIP
		skippedNum.Add(1)
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
	log.Printf("insert=%s, n=%d", db.insert, db.n)
	if _, err := db.insert.Exec(rec.App, rec.Type, rec.When, rec.SessionID,
		rec.Text, rec.EventID, rec.Command, rec.Background, rec.RC,
		base64.StdEncoding.EncodeToString(rec.ID()),
	); err != nil {
		return err
	}
	log.Printf("inserted.")
	insertedNum.Add(1)
	return nil
}

func (db *qlStore) Search(after, before time.Time) (Enumerator, error) {
	rows, err := db.DB.Query(`
    SELECT F_app, F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc
        FROM T_log WHERE F_date >= $1 AND F_date <= $2`, after, before)
	if err != nil {
		return nil, err
	}
	return qlRows{rows}, nil
}

type qlRows struct {
	*sql.Rows
}

func (rs qlRows) Scan(rec *record.Record) error {
	return rs.Rows.Scan(&rec.App, &rec.Type, &rec.When, &rec.SessionID, &rec.Text,
		&rec.EventID, &rec.Command, &rec.Background, &rec.RC)
}

func openQlStore(params, appName string) (Store, error) {
	var firstTime, lastTime time.Time
	db, err := sql.Open("ql", params)
	if err != nil {
		return nil, fmt.Errorf("error opening ql db: %v", err)
	}
	empty := true
	tx, e := db.Begin()
	if e != nil {
		return nil, fmt.Errorf("error beginning transaction: %v", e)
	}
	if _, err = tx.Exec(`CREATE TABLE IF NOT EXISTS T_times (F_app string, F_first time, F_last time)`); err != nil {
		return nil, fmt.Errorf("error creating T_times: %v", err)
	}
	tx.Commit()
	if appName != "" {
		row := db.QueryRow(`SELECT formatTime(F_first, "`+time.RFC3339+`"), formatTime(F_last, "`+time.RFC3339+`")
        FROM T_times WHERE F_app == $1`, appName)
		var ft, lt sql.NullString
		if err = row.Scan(&ft, &lt); err != nil || !ft.Valid || !lt.Valid {
			if !ft.Valid {
				row = db.QueryRow(`SELECT formatTime(F_date, "`+time.RFC3339+`") FROM T_log WHERE F_app = $1 ORDER BY F_date LIMIT 1`, appName)
				err = row.Scan(&ft)
			}
			if !lt.Valid {
				row = db.QueryRow(`SELECT formatTime(F_date, "`+time.RFC3339+`") FROM T_log WHERE F_app = $1 ORDER BY F_date DESC LIMIT 1`, appName)
				err = row.Scan(&lt)
			}
		}
		if err == nil {
			if ft.Valid {
				if firstTime, err = time.Parse(time.RFC3339, ft.String); err != nil {
					log.Fatalf("error parsing %s: %v", ft, err)
				}
				empty = false
			}
			if lt.Valid {
				if lastTime, err = time.Parse(time.RFC3339, lt.String); err != nil {
					log.Fatalf("error parsing %s: %v", lt, err)
				}
				empty = false
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
		/*
			if _, err = tx.Exec(`CREATE UNIQUE INDEX U_log ON T_log(F_id)`); err != nil {
				log.Printf("error creating U_log: %v", err)
			}
			if _, err = tx.Exec(`CREATE INDEX K_log_date ON T_log(F_date)`); err != nil {
				log.Printf("error creating K_log_date: %v", err)
			}
		*/
		tx.Commit()
	}
	return &qlStore{
		DB:      db,
		first:   firstTime,
		last:    lastTime,
		appName: appName,
		insertQry: `
INSERT INTO T_log (F_app, F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc, F_id)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
	}, nil
}
