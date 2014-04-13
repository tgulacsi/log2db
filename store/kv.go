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
	"bytes"
	"compress/flate"
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"unosoft.hu/log2db/record"

	"github.com/cznic/kv"
	"github.com/tgulacsi/lock"
)

const (
	recPrefix    = '!'
	appTimPrefix = '#'
	chunkPrefix  = '$'
	timesPrefix  = '@'

	maxKVLength          = 65787
	compressionThreshold = 4096
	chunkAddrLength      = 1 + sha1.Size
)

var errTooLong = errors.New("too long")

type kvStore struct {
	*kv.DB
	first, last, act time.Time
	appName          string
}

// Open opens the filename
func OpenKVStore(filename, appName string) (Store, error) {
	createOpen := kv.Open
	verb := "opening"
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		createOpen = kv.Create
		verb = "creating"
	}
	opts := &kv.Options{
		VerifyDbBeforeOpen: true, VerifyDbAfterOpen: true,
		VerifyDbBeforeClose: true, VerifyDbAfterClose: true,
		Locker: func(dbname string) (io.Closer, error) {
			lkfile := dbname + ".lock"
			cl, err := lock.Lock(lkfile)
			if err != nil {
				return nil, fmt.Errorf("failed to acquire lock on %s: %v", lkfile, err)
			}
			return cl, nil
		},
	}
	db, err := createOpen(filename, opts)
	log.Printf("db==%#v, err=%v", db, err)
	if err != nil {
		return nil, fmt.Errorf("error %s %s: %v", verb, filename, err)
	}

	if db == nil {
		return nil, fmt.Errorf("nil db!")
	}

	store := &kvStore{DB: db, appName: appName}
	if appName != "" {
		if err := getAppTime(&store.first, db, appName, true); err != nil {
			return nil, fmt.Errorf("error getting first time for %s: %v", appName, err)
		}
		if err := getAppTime(&store.last, db, appName, false); err != nil {
			return nil, fmt.Errorf("error getting last time for %s: %v", appName, err)
		}
	}

	return store, nil
}

func getAppTime(dest *time.Time, db *kv.DB, appName string, first bool) error {
	key, closer := timeKey(appName, first)
	defer closer()
	enum, hit, err := db.Seek(key)
	if err != nil {
		return err
	}
	if hit {
		_, v, err := enum.Next()
		if err != nil {
			return err
		}
		if dest.GobDecode(v); err != nil {
			return err
		}
	} else if first {
		log.Printf("WARN: no first time found for %s, walking!", appName)
		_ = findFirstTime(dest, db, appName)
	}
	return nil
}

func findFirstTime(dest *time.Time, db *kv.DB, appName string) error {
	key, closer := getKeyFor(time.Time{}, appName)
	defer closer()
	enum, _, err := db.Seek(key)
	if err != nil {
		log.Printf("error searching for first app record: %v", err)
		return nil
	}
	k, _, err := enum.Next()
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Printf("error advancing for first app record: %v", err)
		return nil
	}
	if k[0] != recPrefix {
		log.Printf("wanted first of %s, got %s", key, k)
		return nil
	}
	if len(k) < 17 {
		return fmt.Errorf("key %v is too short (%d)! %q", k, len(k), k)
	}
	if err = record.UnmarshalTime(dest, k[1:]); err != nil {
		return fmt.Errorf("error unmarshaling %q as time: %v", k[1:], err)
	}
	return nil
}

var zHashBytes = make([]byte, 8)

var keyPool = NewBytesPool(4)

func getKeyFor(t time.Time, appName string) ([]byte, func()) {
	key := keyPool.Acquire(64)[:1]
	key[0] = recPrefix
	key = key[0 : 1+len(record.MarshalTime(key[1:cap(key)], t))]
	key = append(key, '|')
	if appName != "" {
		key = append(append(append(key, []byte(appName)...), '|'), zHashBytes...)
	}
	return key, func() { keyPool.Release(key) }
}

var timeKeyPool = NewBytesPool(4)

func timeKey(appName string, first bool) ([]byte, func()) {
	b := timeKeyPool.Acquire(1 + 9 + len(appName))[:1]
	b[0] = timesPrefix
	pref := "last"
	if first {
		pref = "first"
	}
	b = append(b, []byte(pref+"Time:"+appName)...)
	return b, func() { timeKeyPool.Release(b) }
}

var marshalPool = NewBytesPool(8)

func (db *kvStore) Insert(rec record.Record) error {
	//log.Printf("Insert(%s)", rec)
	if db.last.After(rec.When) {
		//log.Printf("SKIPping %s as last=%s", rec, db.last)
		skippedNum.Add(1)
		return nil
	}
	if db.act.Before(rec.When) {
		db.act = rec.When
	}
	key := make([]byte, 1, 65)
	key[0] = recPrefix
	var tooLong []byte
	key = append(key, rec.ID()...)
	_, _, err := db.DB.Put(nil, key,
		func(k, v []byte) ([]byte, bool, error) {
			if v != nil { // already exists
				//log.Printf("SKIPping %+v", rec)
				skippedNum.Add(1)
				return nil, false, nil // leave it as were
			}
			v, e := encodeRec(nil, rec)
			if e != nil {
				if e == errTooLong {
					tooLong = v[1:] // the first byte is 0
				}
				return nil, false, e
			}
			insertedNum.Add(1)
			return v, true, nil // set
		})

	// we are ok till 196Mb of compressed data
	if err == errTooLong && (len(tooLong)+maxKVLength-1)/maxKVLength <= (maxKVLength-1)/chunkAddrLength {
		v := []byte{1}
		chunkKey := make([]byte, chunkAddrLength)
		chunkKey[0] = chunkPrefix
		tLen := 0
		for i := 0; i < len(tooLong); {
			n := maxKVLength
			if i+n > len(tooLong) {
				n = len(tooLong) - i
			}
			h := sha1.Sum(tooLong[i : i+n])
			copy(chunkKey[1:], h[:])
			log.Printf("chunkKey=%c%x", chunkKey[0], chunkKey[1:])

			if _, _, err = db.DB.Put(nil, chunkKey,
				func(k, v []byte) ([]byte, bool, error) {
					if len(v) > 0 && len(v) == n { // exists
						log.Printf("skipping %x", k)
						return nil, false, nil
					}
					log.Printf("adding %d length to %x", len(tooLong[i:i+n]), chunkKey)
					return tooLong[i : i+n], true, nil
				}); err != nil {
				//if err = db.DB.Set(chunkKey, tooLong[i:i+n]); err != nil {
				log.Printf("error setting chunk %q: %v", chunkKey, err)
				return err
			}
			tLen += n
			log.Printf("chunk %c%x", chunkKey[0], chunkKey[1:])
			v = append(v, chunkKey...)
			i += n
		}
		err = db.DB.Set(key, v)
		log.Printf("%q: %d (%x: %v)", key, tLen, v, err)
	}
	return err
}

func (db *kvStore) Search(after, before time.Time) (Enumerator, error) {
	afterK, closer := getKeyFor(after, db.appName)
	enum, _, err := db.DB.Seek(afterK)
	if err != nil {
		closer()
		return nil, err
	}
	closer()
	beforeK, _ := getKeyFor(before, db.appName)
	//log.Printf("Search(%q, %q (%s))", afterK, beforeK, before)
	return &kvEnum{DB: db.DB, Enumerator: enum, before: beforeK}, nil
}

type kvEnum struct {
	*kv.DB
	*kv.Enumerator
	before []byte
	last   []byte
	err    error
}

var enumPool = NewBytesPool(16)

func (en *kvEnum) Next() bool {
	if en.err != nil {
		return false
	}
	var key []byte
	for {
		key, en.last, en.err = en.Enumerator.Next()
		if en.err != nil {
			if en.err == io.EOF {
				return false
			}
			log.Printf("key=%x err=%v", key, en.err)
			return false
		}
		if len(key) == 0 {
			continue
		}
		if len(en.before) > 0 {
			n := minInt(len(key), len(en.before))
			if n >= len(en.before) && bytes.Compare(key[:n], en.before) > 0 {
				en.err = io.EOF
				return false
			}
		}
		if len(en.last) == 0 {
			continue
		}
		return true
	}
}

func (en *kvEnum) Scan(rec *record.Record) error {
	if en.err != nil {
		return en.err
	}
	if len(en.last) < 1 || en.last[0] == '{' {
		return json.Unmarshal(en.last, rec)
	}
	k := 100
	if len(en.last) < 100 {
		k = len(en.last)
	}
	log.Printf("last=%x", en.last[:k])
	if en.last[0] == 0 {
		// compressed
		return json.NewDecoder(flate.NewReader(bytes.NewReader(en.last[1:]))).Decode(rec)
	}
	if en.last[0] != 1 {
		return fmt.Errorf("unknown byte head %d (should be 1)", en.last[0])
	}

	// list of compressed chunks
	var (
		err    error
		n      = (len(en.last) - 1) / chunkAddrLength
		key    = make([]byte, chunkAddrLength)
		chunks = make([]io.Reader, 0, n)
	)
	log.Printf("reading from %d chunks", n)
	tLen := 0
	addresses := en.last[1:]
	for i := 0; i < n; i++ {
		key = addresses[i*chunkAddrLength : (i+1)*chunkAddrLength]
		log.Printf("retrieving %c%x", key[0], key[1:])
		val := enumPool.Acquire(maxKVLength)
		copy(val[:10], []byte("UNWRITTEN"))
		if val, err = en.DB.Get(val, key); err != nil {
			log.Printf("error retrieving chunk %q: %v", key, err)
			return err
		}
		if val == nil {
			log.Printf("%c%x is nil!", key[0], key[1:])
			continue
		} else if len(val) == 0 {
			log.Printf("%s%x is zero length? (val=%q %d %d)",
				key[0], key[1:], val, len(val), cap(val))
			continue
		}
		chunks = append(chunks, bytes.NewReader(val))
		tLen += len(val)
		defer func() { enumPool.Release(val) }()
	}
	log.Printf("assembled total length: %d", tLen)
	return json.NewDecoder(flate.NewReader(io.MultiReader(chunks...))).Decode(rec)
}

func (db *kvStore) Close() error {
	commitErr := db.DB.Commit()
	if commitErr != nil {
		log.Printf("error commiting: %v", commitErr)
	}
	if err := db.SaveTimes(); err != nil {
		log.Printf("error saving time: %v", err)
	}
	closeErr := db.DB.Close()
	if commitErr != nil {
		return commitErr
	}
	return closeErr
}

func (db *kvStore) SaveTimes() error {
	if err := db.DB.BeginTransaction(); err != nil {
		return err
	}
	defer db.DB.Commit()
	for _, first := range []bool{false, true} {
		t := db.act
		if first {
			t = db.first
			if t.IsZero() {
				if err := findFirstTime(&t, db.DB, db.appName); err != nil {
					log.Printf("error finding first time for %s: %v", db.appName, err)
				} else {
					db.first = t
				}
			}
		}
		v, err := t.GobEncode()
		if err != nil {
			log.Printf("error encoding %s: %v", t, err)
			return err
		} else {
			key, closer := timeKey(db.appName, first)
			defer closer()
			if err = db.DB.Set(key, v); err != nil {
				log.Printf("error setting %t time for %s: %v", first, db.appName, err)
				return err
			}
			log.Printf("set time %t for %s: %s", first, db.appName, t)
		}
	}
	return nil
}

func encodeRec(b []byte, rec record.Record) ([]byte, error) {
	var e error
	var w io.WriteCloser

	cb := bytes.NewBuffer(b)
	if len(rec.Text) < compressionThreshold {
		w = struct {
			io.Writer
			io.Closer
		}{cb, ioutil.NopCloser(nil)}
	} else {
		cb.WriteByte(0)
		w, e = flate.NewWriter(cb, flate.BestCompression)
		if e != nil {
			e = fmt.Errorf("error creating compressor: %v", e)
			log.Println(e.Error())
			return nil, e
		}
	}

	if e = json.NewEncoder(w).Encode(rec); e != nil {
		log.Printf("error marshaling %+v: %v", rec, e)
		return nil, e
	}
	if e = w.Close(); e != nil {
		e = fmt.Errorf("error writing to compressor: %v", e)
		log.Println(e.Error())
		return nil, e
	}
	if cb.Len() > maxKVLength {
		log.Printf("TOO LONG (%d, max %d)", cb.Len(), maxKVLength)
		return cb.Bytes(), errTooLong
	}
	return cb.Bytes(), nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
