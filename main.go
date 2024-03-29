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
	"errors"
	"expvar"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
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

	"code.google.com/p/go.text/encoding"
	"github.com/tgulacsi/go/text"
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
		flagCharset := flag.String("charset", "utf-8", "source file charset")
		flagLocation := flag.String("location", "Europe/Budapest", "timezone location (for non-zoned record times)")
		flagShuffle := flag.Bool("shuffle", false, "shuffle the record Text (only for tests)")
		flag.Parse()
		parsers.Debug = *flagDebug
		if flag.NArg() < 2 {
			flag.Usage()
		}
		if err := log2db(*flagDB, flag.Arg(0), flag.Arg(1), *flagFilePrefix, *flagCharset, *flagLocation, *flagShuffle); err != nil {
			log.Fatal(err)
		}
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

func log2db(dbURI, appName, logDir, prefix, charset, locName string, shuffle bool) error {
	var loc *time.Location
	if locName != "" {
		var err error
		loc, err = time.LoadLocation(locName)
		if err != nil {
			return fmt.Errorf("unknown location %q: %v", locName, err)
		}
	}
	var enc encoding.Encoding
	if charset != "" {
		charset = strings.ToLower(strings.TrimSpace(charset))
		if charset != "" && charset != "utf-8" && charset != "utf8" {
			if enc = text.GetEncoding(charset); enc == nil {
				return fmt.Errorf("unknown charset %q", charset)
			}
		}
	}

	if prefix == "" {
		prefix = appName
	}
	var producers, consumers sync.WaitGroup

	db, err := openDbURI(appName, dbURI)
	if err != nil {
		return fmt.Errorf("error opening %q: %v", dbURI, err)
	}
	log.Printf("db=%+v", db)
	records := make(chan record.Record, 8*concurrency)
	consumers.Add(1)
	if db == nil {
		if dbURI != "" {
			return fmt.Errorf("%q => nil DB!", dbURI)
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
					if shuffle {
						rec.Text = shuffleString(rec.Text)
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
	errs := make([]string, 0, 1)
	errch := make(chan error, 1)
	go func() {
		for err := range errch {
			e := fmt.Sprintf("ERROR %v", err)
			log.Println(e)
			errs = append(errs, e)
		}
	}()
	log.Printf("reading files of %s from %s", prefix, logDir)
	filesch := readFiles(errch, logDir, prefix, enc)

	if *flagMemprofile != "" {
		for _ = range time.Tick(10 * time.Second) {
			// stop after one round
			f, err := os.Create(*flagMemprofile)
			if err != nil {
				return err
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
					if err = parsers.ParseServerLog(records, r, logDir, appName, loc); err != nil {
						r.Close()
						errch <- fmt.Errorf("error parsing: %v", err)
						return
					}
				} else {
					if err = parsers.ParseLog(records, r, appName, loc); err != nil {
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
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
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
			db, err = store.OpenOraStore(params, appName, concurrency)
		case "pg":
			db, err = store.OpenPgStore(params, appName, concurrency)
		default:
			return nil, fmt.Errorf("unkown db: %s", dbURI)
		}
		if err != nil {
			return nil, err
		}
		if db == nil {
			return nil, fmt.Errorf("NIL db of %q", dbURI)
		}
	}
	return
}

func readFiles(errch chan<- error, logDir, prefix string, enc encoding.Encoding) <-chan io.ReadCloser {
	filesch := make(chan io.ReadCloser, 2)

	makeDecodingReader := func(r io.ReadCloser) io.ReadCloser {
		return struct {
			io.Reader
			io.Closer
		}{text.NewReplacementReader(r), r}
	}
	if enc != nil {
		makeDecodingReader = func(r io.ReadCloser) io.ReadCloser {
			return struct {
				io.Reader
				io.Closer
			}{text.NewDecodingReader(r, enc), r}
		}
	}

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
			filesch <- makeDecodingReader(r)
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

func shuffleString(txt string) string {
	return string(shuffleBytes([]byte(txt)))
}

func shuffleBytes(b []byte) []byte {
	switch len(b) {
	case 0, 1:
		return b
	}
	m := len(b) % 2
	h := (len(b) - m) / 2
	for i, j := range rand.Perm(h) {
		b[m+i], b[m+h+j] = b[m+h+j], b[m+i]
	}
	return b
}

type byMTime []os.FileInfo

func (a byMTime) Len() int           { return len(a) }
func (a byMTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byMTime) Less(i, j int) bool { return a[i].ModTime().Before(a[j].ModTime()) }
