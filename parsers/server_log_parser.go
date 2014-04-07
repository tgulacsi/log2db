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
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"unosoft.hu/log2db/record"
)

// ParseServerLog parses a server.log from the reader,
// returning the record.Records into the dest channel.
func ParseServerLog(dest chan<- record.Record, r io.Reader, appName string) error {
	/*
	   DIR [2013-11-22 11:01:00]: (344194853) $BRUNO_HOME/data/in/elektr
	   SHELL [2013-11-22 11:03:11]: (344194853) 399384314; '$BRUNO_HOME/bin/E_elektr_load 929206 $BRUNO_HOME/data/in/elektr/'KGFB_105766847_20131116161839_0000333436.txt''; 'E', 0
	   SYSLOG [2013-11-22 11:03:33]: (344194853) 5, 'End function "b0917175" "Fok Imre" "zsbedi" type: "18.9" from: "10.65.25.4"'
	   Starting
	   Ended
	*/
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
		switch rec.Type {
		case record.RtSyslog:
			rec.Text = getApostrophedInner(rec.Text)
		case record.RtShell:
			if err = parseShell(&rec); err != nil {
				log.Printf("error parsing %#v: %v", rec, err)
			}
		}
		dest <- rec
	}
}

func parseShell(rec *record.Record) error {
	line := rec.Text
	i := strings.Index(line, ";")
	if i < 0 {
		return fmt.Errorf("no ';' in %q", line)
	}
	var err error
	if rec.EventID, err = strconv.ParseInt(line[:i], 10, 64); err != nil {
		return fmt.Errorf("cannot parse %q as int: %v", line[:i], err)
	}
	i++
	j := strings.LastIndex(line[i:], ";")
	if j < 0 {
		return fmt.Errorf("no second ';' in %q", line)
	}
	j = i + j
	rec.Command = getApostrophedInner(line[i:j])
	j++
	rec.Background, rec.RC = false, uint8(0)
	rcb := line[j:]
	k := strings.Index(rcb, ",")
	if k > 0 {
		rec.Background = getApostrophedInner(rcb[:k])[0] == 'H'
		k = j + k
		rcb = line[k+1:]
	}
	res, err := strconv.Atoi(strings.TrimSpace(rcb))
	if err != nil {
		return fmt.Errorf("cannot parse %q as int (rc): %v", rcb, err)
	}
	rec.RC = uint8(res)
	return nil
}

func getApostrophedInner(line string) string {
	if i := strings.Index(line, "'"); i >= 0 {
		if j := strings.LastIndex(line, "'"); j >= 0 && i < j {
			line = line[i+1 : j]
		}
	}
	return line
}
