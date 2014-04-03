/*
Copyright 2013 Tamás Gulácsi

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
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"
)

// ErrUnfinished is the error for unfinished log record
var ErrUnfinished = errors.New("unfinished")

// ParseServerLog parses a server.log from the reader,
// returning the Records into the dest channel.
func ParseServerLog(dest chan<- Record, r io.Reader) error {
	/*
	   DIR [2013-11-22 11:01:00]: (344194853) $BRUNO_HOME/data/in/elektr
	   SHELL [2013-11-22 11:03:11]: (344194853) 399384314; '$BRUNO_HOME/bin/E_elektr_load 929206 $BRUNO_HOME/data/in/elektr/'KGFB_105766847_20131116161839_0000333436.txt''; 'E', 0
	   SYSLOG [2013-11-22 11:03:33]: (344194853) 5, 'End function "b0917175" "Fok Imre" "zsbedi" type: "18.9" from: "10.65.25.4"'
	   Starting
	   Ended
	*/
	defer close(dest)

	scn := bufio.NewScanner(bufio.NewReader(r))
	var (
		b       []byte
		buf     = bytes.NewBuffer(make([]byte, 0, 1024))
		hadRest bool
		err     error
		rec     Record
	)
	for scn.Scan() {
		if buf.Len() > 0 {
			buf.Write(scn.Bytes())
			b = buf.Bytes()
			hadRest = true
		} else {
			b = scn.Bytes()
			hadRest = false
		}
		log.Printf("line: %q", b)
		if err = parse(&rec, b); err == nil {
			buf.Reset()
			if rec.Type == RT_UNKNOWN {
				dest <- rec
				rec.Type = RT_UNKNOWN
			}
			continue
		}
		if !hadRest {
			buf.Write(scn.Bytes())
		}
	}
	return scn.Err()
}

func parse(rec *Record, b []byte) error {
	i := bytes.IndexByte(b, byte(' '))
	if i >= 0 {
		word, rest := b[:i], b[i+1:]
		switch string(word) {
		case "DIR":
			if err := parseDir(rec, rest); err != nil {
				return fmt.Errorf("error parsing %s: %v", rest, err)
			}
			return nil
		case "SYSLOG":
			if err := parseSyslog(rec, rest); err != nil {
				return fmt.Errorf("error parsing %s as syslog: %v", rest, err)
			}
			return nil
		case "SHELL":
			if err := parseShell(rec, rest); err != nil {
				return fmt.Errorf("error parsing %s as shell: %v", rest, err)
			}
			return nil
		}
	}
	return ErrUnfinished
}

func parseDir(rec *Record, line []byte) error {
	rest, err := parseTimeSID(&rec.When, &rec.SessionID, line)
	if err != nil {
		return err
	}
	rec.Type = RT_DIR
	rec.Text = string(rest)
	return nil
}

func parseSyslog(rec *Record, line []byte) error {
	rest, err := parseTimeSID(&rec.When, &rec.SessionID, line)
	if err != nil {
		return err
	}
	rec.Type = RT_SYSLOG
	rec.Text = string(getApostrophedInner(rest))
	return nil
}

func parseShell(rec *Record, line []byte) error {
	rest, err := parseTimeSID(&rec.When, &rec.SessionID, line)
	if err != nil {
		return err
	}
	line = rest
	rec.Type = RT_SHELL
	rec.Text = ""
	i := bytes.IndexByte(rest, byte(';'))
	if i < 0 {
		return fmt.Errorf("no ';' in %q", line)
	}
	if rec.EventID, err = strconv.ParseInt(string(line[:i]), 10, 64); err != nil {
		return fmt.Errorf("cannot parse %q as int: %v", line[:i], err)
	}
	i++
	j := bytes.LastIndex(line[i:], []byte(";"))
	if j < 0 {
		return fmt.Errorf("no second ';' in %q", line)
	}
	j = i + j
	rec.Command = string(getApostrophedInner(line[i:j]))
	j++
	rec.Background, rec.RC = false, uint8(0)
	rcb := line[j:]
	k := bytes.IndexByte(rcb, byte(','))
	if k > 0 {
		k = j + k
		rec.Background = getApostrophedInner(rcb[:k])[0] == byte('H')
		rcb = line[k+1:]
	}
	res, err := strconv.Atoi(string(rcb))
	if err != nil {
		return fmt.Errorf("cannot parse %q as int (rc): %v", rcb, err)
	}
	rec.RC = uint8(res)
	return nil
}

func getApostrophedInner(line []byte) []byte {
	if i := bytes.IndexByte(line, byte('\'')); i >= 0 {
		if j := bytes.LastIndex(line, []byte("'")); j >= 0 && i < j {
			line = line[i+1 : j]
		}
	}
	return line
}

func parseTimeSID(tim *time.Time, sid *int64, line []byte) ([]byte, error) {
	i := bytes.IndexByte(line, byte('['))
	if i < 0 {
		return line, fmt.Errorf("cannot find '[' in line %q", line)
	}
	i++
	j := bytes.Index(line[i:], []byte("]: ("))
	if j < 0 {
		return line, fmt.Errorf("cannot find ']: (' in line %q", line[i:])
	}
	j = i + j
	var err error
	if *tim, err = time.Parse("2006-01-02 15:04:05", string(line[i+1:j])); err != nil {
		return line, fmt.Errorf("error parsing %q as time: %v", line[i+1:j], err)
	}
	k := bytes.Index(line[j+5:], []byte(") "))
	if k < 0 {
		return line, fmt.Errorf("cannot find ') ' in line %q", line[j+5:])
	}
	k = j + 5 + k
	if *sid, err = strconv.ParseInt(string(line[j:k]), 10, 64); err != nil {
		return line, fmt.Errorf("error parsing %q as sid: %v", line[j:k], err)
	}
	return line[k+2:], nil
}

// RecordType designates the record type (DIR/SHELL/SYSLOG...)
type RecordType uint8

const (
	RT_UNKNOWN = iota
	RT_DIR
	RT_SHELL
	RT_SYSLOG
	RT_OTHER
)

// Record is the structure of one log record
type Record struct {
	Type       RecordType `json:"t"`
	When       time.Time  `json:"d"`
	SessionID  int64      `json:"sid"`
	Text       string     `json:"text"`
	EventID    int64      `json:"evid"`
	Command    string     `json:"cmd"`
	Background bool       `json:"bg"`
	RC         uint8      `json:"rc"`
}
