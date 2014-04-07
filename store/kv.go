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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/camlistore/lock"
	"github.com/cznic/kv"
	"unosoft.hu/log2db/record"
)

const (
	recPrefix    = '!'
	appTimPrefix = '#'
	timesPrefix  = '@'

	maxKVLength = 65787
)

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
		return fmt.Errorf("error unmarshaling %v as time: %v", err)
	}
	return nil
}

var zHashBytes = make([]byte, 8)

var keyPool = NewBytesPool(4)

func getKeyFor(t time.Time, appName string) ([]byte, func()) {
	key := keyPool.Acquire(64)[:1]
	key[0] = recPrefix
	key = key[0 : 1+len(record.MarshalTime(key[1:cap(key)], time.Time{}))]
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
	key = append(key, rec.ID()...)
	_, _, err := db.DB.Put(nil, key,
		func(k, v []byte) ([]byte, bool, error) {
			if v == nil { // not exists
				var e error
				if v, e = json.Marshal(rec); e != nil {
					log.Printf("error marshaling %+v: %v", rec, e)
					return nil, false, e
				} else if len(v) > maxKVLength {
					e = fmt.Errorf("error inserting %+v: too long (%d, max %d)", rec, len(v), maxKVLength)
					log.Println(e.Error())
					return nil, false, e
				}
				insertedNum.Add(1)
				return v, true, nil // set
			}
			//log.Printf("SKIPping %+v", rec)
			skippedNum.Add(1)
			return nil, false, nil // leave it as were
		})
	return err
}

func (db *kvStore) Search(after, before time.Time) (Enumerator, error) {
	key, closer := getKeyFor(after, db.appName)
	enum, _, err := db.DB.Seek(key)
	if err != nil {
		closer()
		return nil, err
	}
	closer()
	key, _ = getKeyFor(before, db.appName)
	return &kvEnum{Enumerator: enum, before: key}, nil
}

type kvEnum struct {
	*kv.Enumerator
	before []byte
	last   []byte
	err    error
}

func (en *kvEnum) Next() bool {
	var key []byte
	key, en.last, en.err = en.Enumerator.Next()
	if en.err != nil {
		return false
	}
	if bytes.Compare(key[:len(en.before)], en.before) > 0 {
		en.err = io.EOF
		return false
	}
	return true
}

func (en *kvEnum) Scan(rec *record.Record) error {
	if en.err != nil {
		return en.err
	}
	return json.Unmarshal(en.last, rec)
}

func (db *kvStore) Close() error {
	log.Printf("closing %s", db)
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
				log.Printf("error setting %t time for %s: %v", first, db.appName)
				return err
			}
			log.Printf("set time %t for %s: %s", first, db.appName, t)
		}
	}
	return nil
}
