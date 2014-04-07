// +build oracle

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
	"sync"
	"time"

	"unosoft.hu/log2db/record"

	//"github.com/tgulacsi/gocilib"
	_ "github.com/tgulacsi/gocilib/driver"
)

func init() {
	OpenOraStore = openOraStore
}

type oraStore struct {
	*sql.DB
	appName          string
	n                int
	first, last, act time.Time
	insertQry        string
	insert           *sql.Stmt
	tx               *sql.Tx
	sync.Mutex
}

func (db *oraStore) snapshot() (*sql.Stmt, error) {
	var err error
	if db.tx != nil {
		db.tx.Commit()
	}
	if db.insert != nil {
		db.insert.Close()
	}
	if db.tx, err = db.Begin(); err != nil {
		return nil, fmt.Errorf("error beginning transaction: %v", err)
	}
	db.insert, err = db.tx.Prepare(db.insertQry)
	if err != nil {
		return nil, fmt.Errorf("error preparing insert: %v", err)
	}
	return db.insert, nil
}

func (db *oraStore) Close() error {
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

func (db *oraStore) SaveTimes() error {
	tx, err := db.DB.Begin()
	if err != nil {
		return err
	}
	log.Println("Deleting T_last")
	if _, err = tx.Exec("DELETE FROM W_GT_times WHERE F_app = ?", db.appName); err != nil {
		log.Printf("error DELETing: %v", err)
		tx.Rollback()
		return err
	}
	log.Printf("inserting last time for %s: %s", db.appName, db.act)
	if _, err = tx.Exec("INSERT INTO W_GT_times (F_app, F_first, F_last) VALUES (?, ?, ?)",
		db.appName, db.first, db.act,
	); err != nil {
		tx.Rollback()
		return fmt.Errorf("error setting times: %v", err)
	}
	tx.Commit()
	log.Println("INSERTed")
	return nil
}

func (db *oraStore) Insert(rec record.Record) error {
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
	bg := "N"
	if rec.Background {
		bg = "I"
	}
	if _, err := db.insert.Exec(rec.App, rec.Type, rec.When, rec.SessionID,
		rec.Text, rec.EventID, rec.Command, bg, rec.RC,
		base64.StdEncoding.EncodeToString(rec.ID()),
	); err != nil {
		if strings.Index(err.Error(), "ORA-00001:") < 0 {
			return err
		}
		skippedNum.Add(1)
		return nil
	}
	insertedNum.Add(1)
	return nil
}

func (db *oraStore) Search(after, before time.Time) (Enumerator, error) {
	rows, err := db.DB.Query(`
    SELECT F_app, F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc
        FROM W_GT_log WHERE F_date BETWEEN ? AND ?`, after, before)
	if err != nil {
		return nil, err
	}
	return oraRows{rows}, nil
}

type oraRows struct {
	*sql.Rows
}

func (rs oraRows) Scan(rec *record.Record) error {
	var c string
	if err := rs.Rows.Scan(&rec.App, &rec.Type, &rec.When, &rec.SessionID, &rec.Text,
		&rec.EventID, &rec.Command, &c, &rec.RC); err != nil {
		return err
	}
	rec.Background = c == "I"
	return nil
}

func openOraStore(params, appName string) (Store, error) {
	var firstTime, lastTime time.Time
	db, err := sql.Open("gocilib", params)
	if err != nil {
		return nil, fmt.Errorf("error opening ora db: %v", err)
	}
	empty := true
	log.Println("CREATE TABLE W_GT_times")
	if _, err = db.Exec(`CREATE TABLE W_GT_times (F_app VARCHAR2(20), F_first TIMESTAMP, F_last TIMESTAMP)`); err != nil {
		log.Printf("err=%v", err)
		if strings.Index(err.Error(), "ORA-00955:") < 0 {
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
		if _, err = db.Exec(`CREATE TABLE W_GT_log (
                F_app VARCHAR2(20) NOT NULL,
                F_type NUMBER(3),
                F_date TIMESTAMP NOT NULL,
                F_sid NUMBER(12),
                F_text CLOB,
                F_evid NUMBER(12),
                F_cmd VARCHAR2(1000),
                F_bg CHAR(1),
                F_rc NUMBER(3),
                F_id VARCHAR2(80)
            )`,
		); err != nil {
			////log.Printf("err=%v", err)
			if strings.Index(err.Error(), "ORA-00955:") < 0 {
				return nil, fmt.Errorf("error creating table W_GT_log: %v", err)
			}
		}
		if _, err = db.Exec(`CREATE UNIQUE INDEX WU_log ON W_GT_log(F_id)`); err != nil {
			//log.Printf("err=%v", err)
			if strings.Index(err.Error(), "ORA-00955:") < 0 {
				log.Printf("error creating WU_log: %v", err)
			}
		}
		if _, err = db.Exec(`CREATE INDEX WK_log_date ON W_GT_log(F_date)`); err != nil {
			//log.Printf("err=%v", err)
			if strings.Index(err.Error(), "ORA-00955:") < 0 {
				log.Printf("error creating WK_log_date: %v", err)
			}
		}
	}
	return &oraStore{
		DB:      db,
		first:   firstTime,
		last:    lastTime,
		appName: appName,
		insertQry: `
INSERT INTO W_GT_log (F_app, F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc, F_id)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
	}, nil
}
