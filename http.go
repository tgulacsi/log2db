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
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"unosoft.hu/log2db/parsers"
	"unosoft.hu/log2db/store"
)

func serveSearch(hostport, dbURI string) {
	db, err := openDbURI("", dbURI)
	if err != nil {
		log.Fatal(err)
	}
	if db == nil {
		log.Fatal("no DB")
	}
	p := pager{db}
	http.HandleFunc("/", p.mainPage)
	log.Printf("listening on %s", hostport)
	log.Fatal(http.ListenAndServe(hostport, nil))
}

type pager struct {
	store.Store
}

func (p pager) mainPage(w http.ResponseWriter, r *http.Request) {
	after, err := parseDate(r.FormValue("after"))
	if err != nil {
		http.Error(w, fmt.Sprintf("bad after: %v", err), 400)
		return
	}
	before, err := parseDate(r.FormValue("before"))
	if err != nil {
		http.Error(w, fmt.Sprintf("bad before: %v", err), 400)
		return
	}
	var limit int
	limitS := r.FormValue("limit")
	if limitS != "" {
		limit, _ = strconv.Atoi(limitS)
	}
	if limit <= 0 {
		limit = 100
	}

	w.Header().Add("Content-Type", "text/html")
	fmt.Fprintf(w, `<!DOCTYPE html>
<html lang="en"><head><title>LOG</title><body>
<form>
<div id="search">
<p>After: <input type="datetime-local" name="after" value="%s" required /></p>
<p>Before: <input type="datetime-local" name="before" value="%s" required /></p>
<p>Limit: <input type="integer" name="limit" value="%d" /></p>
<p><input type="submit" value="Search!" name="search" />
</form>
</div>`, after.Format(time.RFC3339), before.Format(time.RFC3339), limit)

	log.Printf("Search(%s, %s)", after, before)
	enum, err := p.Search(after, before)
	if err != nil {
		http.Error(w, err.Error(), 500)
	}
	flush := func() {}
	if flusher, ok := w.(http.Flusher); ok {
		flush = flusher.Flush
	}

	n := 0
	var rec parsers.Record
	for n < limit && enum.Next() {
		if err = enum.Scan(&rec); err != nil {
			log.Printf("error enumerating: %v", err)
			fmt.Fprintf(w, "<br/>\nerror enumerating: %v", err)
			break
		}
		fmt.Fprintf(w, "<p>%s</p>\n", rec)
		n++
		if n%100 == 0 {
			flush()
		}
	}
	fmt.Fprintf(w, `<body></html>`)
}

func parseDate(dt string) (time.Time, error) {
	if dt == "" {
		return time.Time{}, nil
	}
	layout := time.RFC3339
	if len(dt) < len(layout) {
		layout = layout[:len(dt)]
	}
	//log.Printf("time.Parse(%q, %q)", layout, dt)
	return time.Parse(layout, dt)
}
