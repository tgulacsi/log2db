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

package parsers

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	"unosoft.hu/log2db/record"
)

// Debug makes the program print enormous amount of debug output
var Debug bool

func debug(s string, args ...interface{}) {
	if Debug {
		log.Printf(s, args...)
	}
}

// ErrUnfinished is the error for unfinished log record
var ErrUnfinished = errors.New("unfinished")

type parser struct {
	scn               *bufio.Scanner
	buf               *bytes.Buffer
	actTime, lastTime time.Time
	actRT, lastRT     record.RecordType
	actSID, lastSID   int64
	appName           string
}

type Parser interface {
	// Scan scans the next record into the given destination
	Scan(rec *record.Record) error
}

// ParseLog parses a server.log from the reader,
// returning the Records into the dest channel.
func ParseLog(dest chan<- record.Record, r io.Reader, appName string) error {
	scn := NewBasicParser(r, appName)
	for {
		var rec record.Record
		err := scn.Scan(&rec)
		// debug("err=%v", err)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		dest <- rec
	}
}

// NewBasicParser creates a new basic parser
func NewBasicParser(r io.Reader, appName string) Parser {
	p := &parser{scn: bufio.NewScanner(bufio.NewReader(r)),
		buf:     bytes.NewBuffer(make([]byte, 0, 1024)),
		appName: appName}
	p.scn.Split(bufio.ScanLines)
	return p
}

func (p *parser) Scan(rec *record.Record) error {
	var (
		rest []byte
		err  error
	)
	for p.scn.Scan() {
		line := p.scn.Bytes()
		if len(line) == 0 {
			if err = p.scn.Err(); err != nil {
				break
			}
			p.buf.WriteByte('\n')
			continue
		}
		rest, p.actRT, p.actSID, err = parseTime(&p.actTime, line)
		if err != nil {
			debug("cannot parse time from %q: %v", line, err)
		} else { // new record's beginning
			if p.buf.Len() > 0 {
				rec.App = p.appName
				rec.When = p.lastTime
				rec.Type = p.lastRT
				rec.SessionID = p.lastSID
				rec.Text = p.buf.String()
				p.buf.Reset()
				return nil
			}
			p.lastTime, p.lastRT, p.lastSID = p.actTime, p.actRT, p.actSID
			p.buf.Write(rest)
			p.buf.WriteByte('\n')
			continue
		}
		// append to the end
		p.buf.Write(line)
		p.buf.WriteByte('\n')
		// and scan the next line
	}
	if err == nil {
		err = p.scn.Err()
	}
	if err != nil && err != io.EOF {
		return err
	}
	if p.buf.Len() > 0 {
		rec.App = p.appName
		rec.When = p.lastTime
		rec.Type = p.lastRT
		rec.SessionID = p.lastSID
		rec.Text = p.buf.String()
	}
	return io.EOF
}

func parseTime(tim *time.Time, line []byte) ([]byte, record.RecordType, int64, error) {
	var err error
	if bytes.HasPrefix(line, []byte("SYSLOG [")) || bytes.HasPrefix(line, []byte("DIR [")) || bytes.HasPrefix(line, []byte("SHELL [")) {
		return parseServerlogTime(tim, line)
	}
	rt := record.RecordType(record.RtUnknown)
	if line[0] != '2' {
		return line, rt, 0, ErrUnfinished
	}

	i := bytes.IndexByte(line, ' ') // the space after the date part OR the whole datetime
	if i < 0 {
		return line, rt, 0, ErrUnfinished
	}
	var j int
	if i >= 19 { // 2006-01-02T15:04:05.00000
		if len(line) > i+20 && line[i+1] == '2' { // another time
			if *tim, err = time.Parse("2006-01-02 15:04:05", string(line[i+1:i+1+19])); err == nil {
				return line[i+1+20:], rt, 0, nil
			}
		}
		line[10] = ' '
		j = i
		i = 10
	} else {
		j = bytes.IndexByte(line[i+1:], ' ')
		if j < 0 {
			return line, rt, 0, ErrUnfinished
		}
	}
	i += 1 + j
	if i >= 23 {
		if line[19] == ',' {
			line[19] = '.'
		}
		*tim, err = time.Parse("2006-01-02 15:04:05.000", string(line[:23]))
	} else {
		*tim, err = time.Parse("2006-01-02 15:04:05", string(line[:19]))
	}
	if err != nil {
		return line, rt, 0, fmt.Errorf("error parsing %q as time: %v", line[:i], err)
	}
	if len(line) <= i+1 {
		return nil, rt, 0, fmt.Errorf("error parsing %q: i=%d len=%d", i, len(line))
	}
	return line[i+1:], rt, 0, nil
}

func parseServerlogTime(tim *time.Time, line []byte) ([]byte, record.RecordType, int64, error) {
	i := bytes.IndexByte(line, '[')
	rt, sid := record.RecordType(record.RtUnknown), int64(0)
	switch string(line[:i-1]) {
	case "SYSLOG":
		rt = record.RecordType(record.RtSyslog)
	case "SHELL":
		rt = record.RecordType(record.RtShell)
	case "DIR":
		rt = record.RecordType(record.RtDir)
	default:
		rt = record.RecordType(record.RtOther)
	}
	line = line[i+1:]
	i = bytes.Index(line, []byte("]: ("))
	if i < 0 {
		return line, rt, sid, fmt.Errorf("cannot find ']: (' in line %q", line)
	}
	var err error
	if *tim, err = time.Parse("2006-01-02 15:04:05", string(line[:i])); err != nil {
		return line, rt, sid, fmt.Errorf("error parsing %q as time: %v", line[:i], err)
	}
	line = line[i+5:]
	i = bytes.Index(line, []byte(") "))
	if i < 0 {
		return line, rt, sid, fmt.Errorf("cannot find ') ' in line %q", line)
	}
	if sid, err = strconv.ParseInt(string(line[:i]), 10, 64); err != nil {
		return line, rt, sid, fmt.Errorf("error parsing %q as sid: %v", line[:i], err)
	}
	return line[i+2:], rt, sid, nil
}

func DecomprOpen(fn string) (io.ReadCloser, error) {
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
