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

package record

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"time"
)

// RecordType designates the record type (DIR/SHELL/SYSLOG...)
type RecordType uint8

const (
	RtUnknown = RecordType(iota)
	RtDir
	RtShell
	RtSyslog
	RtOther

	compactLayout = time.RFC3339Nano
)

// Record is the structure of one log record
type Record struct {
	App        string     `json:"a"`
	Type       RecordType `json:"t"`
	When       time.Time  `json:"d"`
	SessionID  int64      `json:"sid"`
	Text       string     `json:"text"`
	EventID    int64      `json:"evid"`
	Command    string     `json:"cmd"`
	Background bool       `json:"bg"`
	RC         uint8      `json:"rc"`
}

// ID returns an app-time-value unique id for this record
func (rec *Record) ID() []byte {
	h := fnv.New64a()
	buf := make([]byte, len(compactLayout)+1+len(rec.App)+1+h.Size())
	n := copy(buf, rec.When.Format(compactLayout))
	buf[n] = '|'
	n++
	k := copy(buf[n:], rec.App)
	n += k
	buf[n] = '|'
	n++
	if err := json.NewEncoder(h).Encode(rec); err != nil {
		log.Printf("error encoding %v: %v", rec, err)
	}
	writeUint64(buf[n:n+h.Size()], h.Sum64())
	n += h.Size()
	return buf[:n]
}

func writeUint64(dest []byte, n uint64) {
	for i := uint(0); i < uint(len(dest)); i++ {
		dest[i] = uint8((n >> (i * 8)) & 0xff)
	}
}
func MarshalTime(dest []byte, t time.Time) []byte {
	return append(dest[:0], []byte(t.Format(compactLayout))...)
}

func UnmarshalTime(t *time.Time, src []byte) error {
	if len(src) < len(compactLayout) {
		return fmt.Errorf("source is too short: %d (want %d)", len(src), len(compactLayout))
	}
	if i := bytes.IndexByte(src, '|'); i > 0 {
		src = src[:i]
	}
	var err error
	*t, err = time.Parse(compactLayout, string(src))
	return err
}
