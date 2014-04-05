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
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/cznic/ql/driver"
	"unosoft.hu/log2db/parsers"
)

var (
	flagIdentity = flag.String("i", "$HOME/.ssh/id_rsa", "ssh identity file")
	flagLogdir   = flag.String("d", "kobed/bruno/data/mai/log", "remote log directory")
	flagLogfile  = flag.String("logfile", "server.log", "main log file")
	flagDebug    = flag.Bool("debug", false, "debug prints")
	flagDestDB   = flag.String("to", "ql://xx.qdb", "destination DB URL")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Használat: %s [options] app_name log_directory\n`, os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	flag.Parse()

	parsers.Debug = *flagDebug
	if flag.NArg() < 2 {
		flag.Usage()
	}
	appName := flag.Arg(0)
	logDir := flag.Arg(1)

	var err error
	var (
		db        *sql.DB
		insertQry string
		lastTime  time.Time
		wg        sync.WaitGroup
	)
	if *flagDestDB != "" {
		i := strings.Index(*flagDestDB, "://")
		driverName, params := (*flagDestDB)[:i], (*flagDestDB)[i+3:]
		switch driverName {
		case "ql":
			db, insertQry, lastTime, err = prepareQlDB(params, appName)
			if err != nil {
				log.Fatalf("error opening %s: %v", *flagDestDB, err)
			}
		default:
			log.Fatalf("unkown db: %s", *flagDestDB)
		}
	}
	log.Printf("db=%v", db)
	records := make(chan parsers.Record, 8)
	wg.Add(1)
	if db == nil {
		defer wg.Done()
		go func() {
			for rec := range records {
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
			insert, err := tx.Prepare(insertQry)
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
			log.Printf("start listening for records...")
			for rec := range records {
				//log.Printf("last=%s rec=%s", lastTime, rec.When)
				if !rec.When.After(lastTime) {
					log.Printf("SKIPPING %+v", rec)
					continue
				}
				log.Printf("RECORD %+v", rec)
				if _, err = insert.Exec(appName,
					rec.Type, rec.When, rec.SessionID, rec.Text,
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

	// log files: if a dir exists with a name eq to appName, then from under
	errch := make(chan error, 1)
	go func() {
		for err := range errch {
			log.Fatalf("ERROR %v", err)
		}
	}()
	log.Printf("reading files of %s from %s", appName, logDir)
	filesch := readFiles(errch, logDir, appName)
	for r := range filesch {
		if appName == "server" {
			if err = parsers.ParseServerLog(records, r); err != nil {
				r.Close()
				log.Fatalf("error parsing %q: %v", r, err)
			}
		} else {
			if err = parsers.ParseLog(records, r); err != nil {
				r.Close()
				log.Printf("error parsing %q: %v", r, err)
			}
		}
		r.Close()
	}
	close(records)
	wg.Wait()
	close(errch)
}

func readFiles(errch chan<- error, logDir, appName string) <-chan io.ReadCloser {
	filesch := make(chan io.ReadCloser, 2)

	go func() {
		defer close(filesch)
		subDir := false
		dh, err := os.Open(filepath.Join(logDir, appName))
		if err == nil {
			subDir = true
		} else {
			if dh, err = os.Open(logDir); err != nil {
				errch <- err
				return
			}
		}
		defer dh.Close()
		infos, err := dh.Readdir(-1)
		if err != nil {
			errch <- err
			return
		}
		files := make([]os.FileInfo, 0, 4)
		for _, fi := range infos {
			if subDir && (fi.Name() == "current" || fi.Name()[0] == '@') ||
				strings.HasPrefix(fi.Name(), appName) {
				files = append(files, fi)
			}
		}
		sort.Sort(byMTime(files))

		for _, fi := range files {
			fn := filepath.Join(dh.Name(), fi.Name())
			log.Printf("fn=%q", fn)
			r, err := decomprOpen(fn)
			if err != nil {
				errch <- err
				return
			}
			filesch <- r
		}
	}()
	return filesch
}

func decomprOpen(fn string) (io.ReadCloser, error) {
	fh, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	b := bufio.NewReader(fh)
	for {
		buf, err := b.Peek(8)
		if err != nil {
			fh.Close()
			return nil, err
		}
		if bytes.Equal(buf[:3], []byte("BZh")) {
			cr := bzip2.NewReader(b)
			b = bufio.NewReader(cr)
			continue
		}
		if buf[0] == 0x1f && buf[1] == 0x8b {
			cr, err := gzip.NewReader(b)
			if err != nil {
				return nil, err
			}
			b = bufio.NewReader(cr)
			continue
		}
		break
	}
	return struct {
		io.Reader
		io.Closer
	}{b, fh}, nil
}

type byMTime []os.FileInfo

func (a byMTime) Len() int           { return len(a) }
func (a byMTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byMTime) Less(i, j int) bool { return a[i].ModTime().Before(a[j].ModTime()) }

func prepareQlDB(params, appName string) (db *sql.DB, insertQry string, lastTime time.Time, err error) {
	defer func() {
		log.Printf("sql.Open(ql, %q): %s, %s, %v", params, db, lastTime, err)
	}()
	db, err = sql.Open("ql", params)
	if err != nil {
		err = fmt.Errorf("error opening ql db: %v", err)
		return
	}
	row := db.QueryRow(`SELECT max(formatTime(F_date, "`+time.RFC3339+`"))
        FROM T_log WHERE F_app == $1`, appName)
	var lt sql.NullString
	if err = row.Scan(&lt); err == nil {
		if lt.Valid {
			if lastTime, err = time.Parse(time.RFC3339, lt.String); err != nil {
				log.Fatalf("error parsing %s: %v", lt, err)
			}
		}
	} else {
		log.Printf("%v", err)
		tx, e := db.Begin()
		if e != nil {
			err = fmt.Errorf("error beginning transaction: %v", e)
			return
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
                F_rc int64
            )`,
		); err != nil {
			tx.Rollback()
			err = fmt.Errorf("error creating table T_log: %v", err)
			return
		}
		tx.Commit()
	}
	insertQry = `
INSERT INTO T_log (F_app, F_type, F_date, F_sid, F_text, F_evid, F_cmd, F_bg, F_rc)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	return
}
