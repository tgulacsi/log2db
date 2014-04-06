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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/camlistore/lock"
	"github.com/cznic/kv"
	"unosoft.hu/log2db/parsers"
)

const (
	recPrefix    = '!'
	appTimPrefix = '#'
	lastsPrefix  = '@'
)

type kvStore struct {
	*kv.DB
	last, act time.Time
	appName   string
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
	enum, hit, err := db.Seek(lastTimeKey(appName))
	if err != nil {
		return nil, err
	}
	if hit {
		_, v, err := enum.Next()
		if err != nil {
			return nil, err
		}
		if (&store.last).GobDecode(v); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func lastTimeKey(appName string) []byte {
	b := make([]byte, 1, 1+9+len(appName))
	b[0] = lastsPrefix
	return append(b, []byte("lastTime:"+appName)...)
}

func (db *kvStore) Insert(rec parsers.Record) error {
	if db.last.After(rec.When) {
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
					return nil, false, e
				}
				return v, true, nil // set
			}
			//log.Printf("SKIPping %+v", rec)
			return nil, false, nil // leave it as were
		})
	return err
}

func (db *kvStore) Close() error {
	if err := db.SaveTime(); err != nil {
		log.Printf("error saving time: %v", err)
	}
	commitErr := db.DB.Commit()
	closeErr := db.DB.Close()
	if commitErr != nil {
		return commitErr
	}
	return closeErr
}

func (db *kvStore) SaveTime() error {
	v, err := db.act.GobEncode()
	if err != nil {
		return err
	} else {
		if err = db.DB.Set(lastTimeKey(db.appName), v); err != nil {
			return err
		}
	}
	return nil
}
