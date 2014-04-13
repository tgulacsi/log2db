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
	"crypto/rand"
	"encoding/base64"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"unosoft.hu/log2db/record"
)

func TestKvStore(t *testing.T) {
	dn, err := ioutil.TempDir("", "kv-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dn)
	st, err := OpenKVStore(filepath.Join(dn, "kv.db"), "test")
	if err != nil {
		t.Fatal(err)
	}
	rnd := make([]byte, maxKVLength)
	if _, err := rand.Read(rnd); err != nil {
		t.Errorf("error reading random: %v", err)
	}
	for i, txt := range []string{
		"short text",
		strings.Repeat("A", maxKVLength),
		strings.Repeat("B", maxKVLength+1),
		strings.Repeat("C", 10) + strings.Repeat(base64.StdEncoding.EncodeToString(rnd), 10),
	} {
		tim := time.Now().Add(-time.Duration(i) * time.Second)
		rec := record.Record{When: tim, Text: txt}
		if err = st.Insert(rec); err != nil {
			t.Fatal(err)
		}
		enum, err := st.Search(tim, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if !enum.Next() {
			t.Fatal("enum is empty")
		}
		rec2 := new(record.Record)
		if err = enum.Scan(rec2); err != nil {
			t.Fatal(err)
		}
		if rec2.Text != rec.Text {
			t.Errorf("bad text: got %q, awaited %q", rec2.Text, rec.Text)
		}
	}
}

func TestKvEncodeRec(t *testing.T) {
	tim := time.Now().Add(-3 * time.Second)
	for i, txt := range []string{
		"short text",
		strings.Repeat("A", maxKVLength),
		strings.Repeat("B", maxKVLength+1),
	} {
		rec := record.Record{When: tim, Text: txt}
		v, e := encodeRec(nil, rec)
		if e != nil {
			t.Errorf("%d. error encoding %q: %v", i, rec, e)
			continue
		}
		if len(v) == 0 {
			t.Errorf("%d. nil encoding!", i)
		}
	}
}
