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
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"unosoft.hu/log2db/parsers"
	"unosoft.hu/log2db/store"
)

var (
	flagIdentity   = flag.String("i", "$HOME/.ssh/id_rsa", "ssh identity file")
	flagLogdir     = flag.String("d", "kobed/bruno/data/mai/log", "remote log directory")
	flagLogfile    = flag.String("logfile", "server.log", "main log file")
	flagDebug      = flag.Bool("debug", false, "debug prints")
	flagVerbose    = flag.Bool("verbose", false, "print input records")
	flagDestDB     = flag.String("to", "ql://xx.qdb", "destination DB URL")
	flagFilePrefix = flag.String("prefix", "", "filename's prefix - defaults to the app, if not given")
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
	prefix := *flagFilePrefix
	if prefix == "" {
		prefix = appName
	}

	var err error
	var (
		db store.Store
		wg sync.WaitGroup
	)
	if *flagDestDB != "" {
		i := strings.Index(*flagDestDB, "://")
		driverName, params := (*flagDestDB)[:i], (*flagDestDB)[i+3:]
		switch driverName {
		case "ql":
			db, err = store.OpenQlStore(params, appName)
			if err != nil {
				log.Fatalf("error opening %s: %v", *flagDestDB, err)
			}
		case "kv":
			db, err = store.OpenKVStore(params, appName)
		default:
			log.Fatalf("unkown db: %s", *flagDestDB)
		}
	}
	log.Printf("db=%+v", db)
	records := make(chan parsers.Record, 8)
	wg.Add(1)
	if db == nil {
		defer wg.Done()
		go func() {
			for rec := range records {
				if *flagVerbose {
					log.Printf("RECORD %+v", rec)
				}
			}
		}()
	} else {
		defer db.Close()
		go func() {
			defer wg.Done()
			log.Printf("start listening for records...")
			for rec := range records {
				//log.Printf("last=%s rec=%s", lastTime, rec.When)
				if *flagVerbose {
					log.Printf("RECORD %+v", rec)
				}
				if err = db.Insert(rec); err != nil {
					log.Printf("error inserting record: %v", err)
					continue
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
	log.Printf("reading files of %s from %s", prefix, logDir)
	filesch := readFiles(errch, logDir, prefix)
	for r := range filesch {
		if appName == "server" {
			if err = parsers.ParseServerLog(records, r, appName); err != nil {
				r.Close()
				log.Fatalf("error parsing %q: %v", r, err)
			}
		} else {
			if err = parsers.ParseLog(records, r, appName); err != nil {
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
		files := make([]os.FileInfo, 0, 4)
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
