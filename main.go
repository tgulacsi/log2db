/*
Copyright 2013 Tamás Gulácsi

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

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/cznic/ql/driver"
	"unosoft.hu/log2db/parsers"
	"unosoft.hu/log2db/ssh"
)

var (
	flagIdentity = flag.String("i", "$HOME/.ssh/id_rsa", "ssh identity file")
	flagLogdir   = flag.String("d", "kobed/bruno/data/mai/log", "remote log directory")
	flagLogfile  = flag.String("logfile", "server.log", "main log file")
	flagParseLog = flag.String("parselog", "", "(TEST) parse given file")
	flagDebug    = flag.Bool("debug", false, "debug prints")
	flagDestDB   = flag.String("db", "ql://xx.qdb", "destination DB URL")
)

func main() {
	flag.Parse()

	parsers.Debug = *flagDebug

	if *flagParseLog != "" {
		dest := make(chan parsers.Record, 8)
		fn := *flagParseLog
		var (
			err error
			fh  *os.File
		)
		if fn == "-" {
			fh = os.Stdin
		} else {
			fh, err = os.Open(fn)
			if err != nil {
				log.Fatalf("cannot open %q: %v", fn, err)
			}
		}
		defer fh.Close()

		var db *sql.DB
		var wg sync.WaitGroup
		if *flagDestDB != "" {
			i := strings.Index(*flagDestDB, "://")
			driverName, params := (*flagDestDB)[:i], (*flagDestDB)[i+3:]
			switch driverName {
			case "ql":
				db, err = prepareQlDB(params)
				if err != nil {
					log.Fatalf("error opening %s: %v", *flagDestDB, err)
				}
			default:
				log.Fatalf("unkown db: %s", *flagDestDB)
			}
		}
		wg.Add(1)
		if db == nil {
			defer wg.Done()
			go func() {
				for rec := range dest {
					log.Printf("RECORD %+v", rec)
				}
			}()
		} else {
			defer db.Close()
			snapshot := func(tx *sql.Tx) (*sql.Tx, *sql.Stmt) {
				if tx != nil {
					tx.Commit()
				}
				if tx, err = db.Begin(); err != nil {
					log.Fatalf("error beginning transaction: %v", err)
				}
				insert, err := tx.Prepare(`
INSERT INTO T_log (F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`)
				if err != nil {
					log.Fatalf("error preparing insert: %v", err)
				}
				return tx, insert
			}
			go func() {
				defer wg.Done()
				n := 0
				tx, insert := snapshot(nil)
				defer func() {
					if tx != nil {
						tx.Commit()
					}
				}()
				for rec := range dest {
					log.Printf("RECORD %+v", rec)
					if _, err = insert.Exec(rec.Type, rec.When, rec.SessionID, rec.Text,
						rec.EventID, rec.Command, rec.Background, rec.RC,
					); err != nil {
						log.Printf("error inserting record: %v", err)
						continue
					}
					n++
					if n%1024 == 0 {
						tx, insert = snapshot(tx)
					}
				}
			}()
		}
		if err = parsers.ParseServerLog(dest, fh); err != nil {
			log.Fatalf("error parsing %q: %v", fn, err)
		}
		wg.Wait()
	} else {

		user := "kobe"
		host := "p520"

		c, err := ssh.NewClient(user, host, os.ExpandEnv(*flagIdentity))
		if err != nil {
			log.Fatalf("cannot create client: %v", err)
		}
		defer c.Close()

		fn := filepath.Join(*flagLogdir, *flagLogfile)
		out, err := c.Tail(fn, 5, true)
		//out, err := c.Cat(fn)
		if err != nil {
			log.Fatalf("cannot cat %q: %v", fn, err)
		}
		defer out.Close()
		io.Copy(os.Stdout, out)
	}
}

func prepareQlDB(params string) (*sql.DB, error) {
	log.Printf("sql.Open(ql, %q)", params)
	db, err := sql.Open("ql", params)
	if err != nil {
		return nil, fmt.Errorf("error opening ql db: %v", err)
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Commit()
	if _, err = tx.Exec(`CREATE TABLE IF NOT EXISTS T_log (
                F_type int64,
                F_date time,
                F_sid int64,
                F_text string,
                F_evid int64,
                F_cmd string,
                F_bg bool,
                F_rc int64
            )`); err != nil {
		return nil, fmt.Errorf("error creating table T_log: %v", err)
	}
	return db, nil
}
