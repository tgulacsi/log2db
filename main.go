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

package main

import (
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	"unosoft.hu/log2db/parsers"
	"unosoft.hu/log2db/record"
	"unosoft.hu/log2db/store"
)

var (
	// global options
	flagLogfile    = flag.String("logfile", "server.log", "main log file")
	flagDebug      = flag.Bool("debug", false, "debug prints")
	flagVerbose    = flag.Bool("verbose", false, "print input records")
	flagMemprofile = flag.String("memprofile", "", "write memory profile to file")
)

var concurrency = runtime.GOMAXPROCS(0)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `Használat: %s [options] <serve|log2db|pull> [args]\n`, os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	if len(os.Args) < 2 {
		flag.Usage()
	}
	cmd := os.Args[1]
	if len(os.Args) > 2 {
		os.Args = append(os.Args[:1], os.Args[2:]...)
	} else {
		os.Args = []string{os.Args[0]}
	}

	switch cmd {
	case "serve":
		flagHTTP := flag.String("http", "localhost:8181", "serve on host:port")
		flagDB := flag.String("db", "kv://log2db.kvdb", "source DB URL")
		flag.Parse()
		serveSearch(*flagHTTP, *flagDB)
	case "log2db":
		flagFilePrefix := flag.String("prefix", "", "filename's prefix - defaults to the app, if not given")
		flagDB := flag.String("db", "kv://log2db.kvdb", "destination DB URL")
		flag.Parse()
		parsers.Debug = *flagDebug
		if flag.NArg() < 2 {
			flag.Usage()
		}
		log2db(*flagDB, flag.Arg(0), flag.Arg(1), *flagFilePrefix)
	case "pull":
		flagIdentity := flag.String("i", "$HOME/.ssh/id_rsa", "ssh identity file")
		flagLogdir := flag.String("d", "kobed/bruno/data/mai/log", "remote log directory")
		flag.Parse()
		log.Printf("not implemented")
		_, _ = *flagIdentity, *flagLogdir
	default:
		flag.Usage()
	}

}

func log2db(dbURI, appName, logDir, prefix string) {
	if prefix == "" {
		prefix = appName
	}
	var producers, consumers sync.WaitGroup

	db, err := openDbURI(appName, dbURI)
	if err != nil {
		log.Fatalf("error opening %q: %v", dbURI, err)
	}
	log.Printf("db=%+v", db)
	records := make(chan record.Record, 8*concurrency)
	consumers.Add(1)
	if db == nil {
		if dbURI != "" {
			log.Fatalf("%q => nil DB!", dbURI)
		}
		go func() {
			defer consumers.Done()
			for rec := range records {
				if *flagVerbose {
					log.Printf("RECORD %+v", rec)
				}
			}
		}()
	} else {
		c := 1
		if strings.HasPrefix(dbURI, "ora://") {
			c = concurrency
		}

		for i := 0; i < c; i++ {
			go func() {
				defer consumers.Done()
				log.Printf("start listening for records...")
				for rec := range records {
					if *flagVerbose {
						log.Printf("RECORD %+v", rec)
					}
					if rec.When.IsZero() {
						continue
					}
					if err := db.Insert(rec); err != nil {
						log.Printf("error inserting record: %v", err)
						continue
					}
				}
			}()
		}
	}

	// log files: if a dir exists with a name eq to appName, then from under
	errch := make(chan error, 1)
	go func() {
		for err := range errch {
			log.Fatalf("ERROR %v", err)
		}
	}()
	log.Printf("reading files of %s from %s", prefix, logDir)
	filesch := readFiles(errch, logDir, prefix)

	if *flagMemprofile != "" {
		for _ = range time.Tick(10 * time.Second) {
			// stop after one round
			f, err := os.Create(*flagMemprofile)
			if err != nil {
				log.Fatal(err)
			}
			pprof.WriteHeapProfile(f)
			f.Close()
		}
	}
	for i := 0; i < concurrency; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for r := range filesch {
				if appName == "server" {
					if err = parsers.ParseServerLog(records, r, logDir, appName); err != nil {
						r.Close()
						log.Fatalf("error parsing: %v", err)
					}
				} else {
					if err = parsers.ParseLog(records, r, appName); err != nil {
						r.Close()
						log.Printf("error parsing: %v", err)
					}
				}
				r.Close()
				if db != nil {
					if err = db.SaveTimes(); err != nil {
						log.Printf("error saving time: %v", err)
					}
				}
			}
		}()
	}
	if db != nil {
		defer db.Close()
	}
	producers.Wait()
	close(records)
	consumers.Wait()
	close(errch)

	log.Printf("skipped %s, inserted %s records.", expvar.Get("storeSkipped"), expvar.Get("storeInserted"))
}

func openDbURI(appName, dbURI string) (db store.Store, err error) {
	if dbURI != "" {
		i := strings.Index(dbURI, "://")
		if i < 0 {
			return nil, fmt.Errorf("dbURI should be in driver://params format, but got %q", dbURI)
		}
		driverName, params := dbURI[:i], dbURI[i+3:]
		switch driverName {
		case "ql":
			db, err = store.OpenQlStore(params, appName)
		case "kv":
			db, err = store.OpenKVStore(params, appName)
		case "ora":
			db, err = store.OpenOraStore(params, appName)
		default:
			log.Fatalf("unkown db: %s", dbURI)
		}
		if err != nil {
			return nil, err
		}
		if db == nil {
			log.Fatalf("NIL db of %q", dbURI)
		}
	}
	return
}

func readFiles(errch chan<- error, logDir, prefix string) <-chan io.ReadCloser {
	filesch := make(chan io.ReadCloser, 2)

	go func() {
		defer close(filesch)
		subDir := false
		dh, err := os.Open(filepath.Join(logDir, prefix))
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
		files := make([]os.FileInfo, 0, concurrency)
		for _, fi := range infos {
			if subDir && (fi.Name() == "current" || fi.Name()[0] == '@') ||
				fnAppPrefix(prefix, fi.Name()) {
				files = append(files, fi)
			}
		}
		sort.Sort(byMTime(files))

		for _, fi := range files {
			fn := filepath.Join(dh.Name(), fi.Name())
			log.Printf("fn=%q", fn)
			r, err := parsers.DecomprOpen(fn)
			if err != nil {
				errch <- err
				return
			}
			filesch <- r
		}
	}()
	return filesch
}

func fnAppPrefix(prefix, fn string) bool {
	if !strings.HasPrefix(fn, prefix) {
		return false
	}
	if len(fn) == len(prefix) {
		return true
	}
	c := fn[len(prefix)]
	if 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '_' {
		return false
	}
	return true
}

type byMTime []os.FileInfo

func (a byMTime) Len() int           { return len(a) }
func (a byMTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byMTime) Less(i, j int) bool { return a[i].ModTime().Before(a[j].ModTime()) }
