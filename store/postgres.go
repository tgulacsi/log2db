// +build postgres

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
	"strings"
	"sync/atomic"
	"time"

	"unosoft.hu/log2db/record"

	_ "github.com/lib/pq"
)

func init() {
	OpenPgStore = openPgStore
}

type pgStore struct {
	db               dbPool
	appName          string
	n                int32
	first, last, act time.Time
}

func (db *pgStore) Close() error {
	if err := db.SaveTimes(); err != nil {
		log.Printf("error saving time: %v", err)
	}
	return db.db.Close()
}

func (db *pgStore) SaveTimes() error {
	tx, release, err := db.db.getTx()
	if err != nil {
		return err
	}
	defer release(false)
	//log.Println("Deleting T_last")
	if _, err = tx.Exec("DELETE FROM W_GT_times WHERE F_app = $1", db.appName); err != nil {
		log.Printf("error DELETing: %v", err)
		return err
	}
	//log.Printf("inserting last time for %s: %s", db.appName, db.act)
	if _, err = tx.Exec("INSERT INTO W_GT_times (F_app, F_first, F_last) VALUES ($1, $2, $3)",
		db.appName, db.first, db.act,
	); err != nil {
		return fmt.Errorf("error setting times: %v", err)
	}
	return tx.Commit()
}

func (db *pgStore) Insert(rec record.Record) error {
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
	insert, release, err := db.db.get()
	if err != nil {
		return err
	}
	bg := "N"
	if rec.Background {
		bg = "I"
	}
	if _, err := insert.Stmt.Exec(rec.App, rec.Type, rec.When, rec.SessionID,
		rec.Text, rec.EventID, rec.Command, bg, rec.RC,
		base64.StdEncoding.EncodeToString(rec.ID()),
	); err != nil {
		insert.Stmt.Close()
		insert.Tx.Commit()
		if strings.Index(err.Error(), "duplicate key") < 0 {
			return err
		}
		return nil
	}
	insertedNum.Add(1)
	return release(atomic.AddInt32(&db.n, 1)%1000 == 0)
}

func (db *pgStore) Search(after, before time.Time) (Enumerator, error) {
	rows, err := db.db.DB.Query(`
    SELECT F_app, F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc
        FROM W_GT_log WHERE F_date BETWEEN $1 AND $2`, after, before)
	if err != nil {
		return nil, err
	}
	return pgRows{rows}, nil
}

type pgRows struct {
	*sql.Rows
}

func (rs pgRows) Scan(rec *record.Record) error {
	var c string
	if err := rs.Rows.Scan(&rec.App, &rec.Type, &rec.When, &rec.SessionID, &rec.Text,
		&rec.EventID, &rec.Command, &c, &rec.RC); err != nil {
		return err
	}
	rec.Background = c == "I"
	return nil
}

func openPgStore(params, appName string, concurrency int) (Store, error) {
	if concurrency < 1 {
		concurrency = 1
	}
	var firstTime, lastTime time.Time
	db, err := sql.Open("postgres", params)
	if err != nil {
		return nil, fmt.Errorf("error opening pg db: %v", err)
	}
	empty := true
	log.Println("CREATE TABLE W_GT_times")
	if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS W_GT_times
            (F_app VARCHAR(20), F_first TIMESTAMP, F_last TIMESTAMP)`); err != nil {
		log.Printf("err=%v", err)
		if !strings.HasSuffix(err.Error(), "already exists") {
			return nil, fmt.Errorf("error creating W_GT_times: %v", err)
		}
	}
	if appName != "" {
		log.Println("SELECT F_first, F_last FROM W_GT_times")
		row := db.QueryRow(`SELECT TO_CHAR(F_first, 'YYYY-MM-DD"T"HH24:MI:SS"Z"'), TO_CHAR(F_last, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
            FROM W_GT_times WHERE F_app == $1`, appName)
		var ft, lt sql.NullString
		if err = row.Scan(&ft, &lt); err == nil {
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
		log.Println("CREATE TABLE W_GT_log")
		if _, err = db.Exec(`CREATE TABLE IF NOT EXISTS W_GT_log (
                F_app VARCHAR(20) NOT NULL,
                F_type NUMERIC(3),
                F_date TIMESTAMP NOT NULL,
                F_sid NUMERIC(12),
                F_text TEXT,
                F_evid NUMERIC(12),
                F_cmd VARCHAR(1000),
                F_bg CHAR(1),
                F_rc NUMERIC(3),
                F_id VARCHAR(80)
            )`,
		); err != nil {
			return nil, fmt.Errorf("error creating table W_GT_log: %v", err)
		}
		if _, err = db.Exec(`CREATE UNIQUE INDEX WU_log ON W_GT_log(F_id)`); err != nil {
			//log.Printf("err=%v", err)
			if !strings.HasSuffix(err.Error(), "already exists") {
				log.Printf("error creating WU_log: %v", err)
			}
		}
		if _, err = db.Exec(`CREATE INDEX WK_log_date ON W_GT_log(F_date)`); err != nil {
			//log.Printf("err=%v", err)
			if !strings.HasSuffix(err.Error(), "already exists") {
				log.Printf("error creating WK_log_date: %v", err)
			}
		}
	}
	return &pgStore{
		db: dbPool{
			DB:     db,
			txPool: make(chan txStmt, concurrency),
			insertQry: `
INSERT INTO W_GT_log (F_app, F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc, F_id)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		},
		first:   firstTime,
		last:    lastTime,
		appName: appName,
	}, nil
}
