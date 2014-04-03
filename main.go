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

package main

import (
	"flag"
	"io"
	"log"
	"os"
	"path/filepath"

	"unosoft.hu/log2db/parsers"
	"unosoft.hu/log2db/ssh"
)

var (
	flagIdentity = flag.String("i", "$HOME/.ssh/id_rsa", "ssh identity file")
	flagLogdir   = flag.String("d", "kobed/bruno/data/mai/log", "remote log directory")
	flagLogfile  = flag.String("logfile", "server.log", "main log file")
	flagParseLog = flag.String("parselog", "", "(TEST) parse given file")
)

func main() {
	flag.Parse()

	if *flagParseLog != "" {
		dest := make(chan parsers.Record, 8)
		fn := *flagParseLog
		var (
			err error
			fh  *os.File
		)
		if fn == "-" {
			fh = os.Stdin
		} else {
			fh, err = os.Open(fn)
			if err != nil {
				log.Fatalf("cannot open %q: %v", fn, err)
			}
		}
		defer fh.Close()
		go func() {
			for rec := range dest {
				log.Printf("RECORD %s", rec)
			}
		}()
		if err = parsers.ParseServerLog(dest, fh); err != nil {
			log.Fatalf("error parsing %q: %v", fn, err)
		}
	} else {

		User := "kobe"
		Host := "p520"

		c, err := ssh.NewClient(User, Host, os.ExpandEnv(*flagIdentity))
		if err != nil {
			log.Fatalf("cannot create client: %v", err)
		}
		defer c.Close()

		fn := filepath.Join(*flagLogdir, *flagLogfile)
		out, err := c.Tail(fn, 5, true)
		//out, err := c.Cat(fn)
		if err != nil {
			log.Fatalf("cannot cat %q: %v", fn, err)
		}
		defer out.Close()
		io.Copy(os.Stdout, out)
	}
}
