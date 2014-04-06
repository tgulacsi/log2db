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

	"github.com/cznic/kv"
	"unosoft.hu/log2db/parsers"
)

const (
	recPrefix    = '!'
	appTimPrefix = '#'
)

type kvStore struct {
	*kv.DB
}

// Open opens the filename
func OpenKVStore(filename string) (Store, error) {
	db, err := kv.Open(filename, &kv.Options{
		VerifyDbBeforeOpen: true, VerifyDbAfterOpen: true,
		VerifyDbBeforeClose: true, VerifyDbAfterClose: true})
	if err != nil {
		return nil, err
	}
	return &kvStore{db}, nil
}

func (db *kvStore) Insert(appName string, rec parsers.Record) error {
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
			return nil, false, nil // leave it as were
		})
	return err
}

func (db *kvStore) Close() error {
	commitErr := db.DB.Commit()
	closeErr := db.DB.Close()
	if commitErr != nil {
		return commitErr
	}
	return closeErr
}
