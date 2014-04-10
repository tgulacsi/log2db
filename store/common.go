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
	"errors"
	"expvar"
	"time"

	"unosoft.hu/log2db/record"
)

type Store interface {
	Insert(rec record.Record) error
	Close() error
	SaveTimes() error
	Search(after, before time.Time) (Enumerator, error)
}

type Enumerator interface {
	Next() bool
	Scan(*record.Record) error
}

var (
	OpenOraStore = func(params, appName string, concurrency int) (Store, error) { return nil, errors.New("not implemented") }
	OpenQlStore  = func(params, appName string) (Store, error) { return nil, errors.New("not implemented") }
)

var (
	skippedNum  = expvar.NewInt("storeSkipped")
	insertedNum = expvar.NewInt("storeInserted")
)

type bytesPool struct {
	ch chan []byte
}

func NewBytesPool(capacity int) bytesPool {
	if capacity <= 0 {
		capacity = 1
	}
	return bytesPool{ch: make(chan []byte, capacity)}
}
func (p bytesPool) Acquire(n int) []byte {
	for {
		select {
		case b := <-p.ch:
			if cap(b) >= n {
				return b[:n]
			}
		default:
			return make([]byte, n)
		}
	}
}

func (p bytesPool) Release(b []byte) {
	select {
	case p.ch <- b:
	default:
	}
}
