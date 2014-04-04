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

// From https://gist.github.com/jedy/3357393
// https://blogs.oracle.com/janp/entry/how_the_scp_protocol_works

package ssh

import (
	"crypto"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"

	"code.google.com/p/go.crypto/ssh"
	"code.google.com/p/go.crypto/ssh/terminal"
)

type keychain struct {
	key *rsa.PrivateKey
}

func (k *keychain) Key(i int) (key ssh.PublicKey, err error) {
	if i != 0 {
		return nil, nil
	}
	return ssh.NewPublicKey(&k.key.PublicKey)
}

func (k *keychain) Sign(i int, rand io.Reader, data []byte) (sig []byte, err error) {
	hashFunc := crypto.SHA1
	h := hashFunc.New()
	h.Write(data)
	digest := h.Sum(nil)
	return rsa.SignPKCS1v15(rand, k.key, hashFunc, digest)
}

func getClient(user, host, privateKeyFilename string) (*ssh.ClientConn, error) {
	auth := make([]ssh.ClientAuth, 0, 2)
	if privateKeyFilename != "" {
		rsakey, err := ReadKey(privateKeyFilename)
		if err != nil {
			return nil, fmt.Errorf("error reading %q: %s", privateKeyFilename, err)
		}
		auth = append(auth, ssh.ClientAuthKeyring(&keychain{rsakey}))
	}
	if os.Getenv("SSH_AUTH_SOCK") != "" {
		sock, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
		if err != nil {
			log.Printf("error connecting to ssh agent: %v", err)
			if len(auth) == 0 {
				os.Exit(1)
			}
		}
		auth = append(auth, ssh.ClientAuthAgent(ssh.NewAgentClient(sock)))
	}
	clientConfig := &ssh.ClientConfig{User: user, Auth: auth}
	if !strings.Contains(host, ":") {
		host = host + ":22"
	}
	conn, err := ssh.Dial("tcp", host, clientConfig)
	if err != nil {
		return nil, fmt.Errorf("getClient: error dialing %q with %s: %v", host, clientConfig, err)
	}
	return conn, nil
}

func ReadKey(filename string) (*rsa.PrivateKey, error) {
	pkbytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("ReadKey: error reading %q: %v", filename, err)
	}
	block, rest := pem.Decode(pkbytes)
	if block == nil {
		return nil, fmt.Errorf("ReadKey: couldn't decode %q", rest)
	}
	der := block.Bytes
	log.Printf("headers: %s", block.Headers)
	if _, ok := block.Headers["DEK-Info"]; ok {
		passw, err := terminal.ReadPassword(int(os.Stdin.Fd()))
		if err != nil {
			log.Fatalf("error getting password: %v", err)
		}
		if der, err = x509.DecryptPEMBlock(block, passw); err != nil {
			return nil, fmt.Errorf("cannot decrypt private key %q: %v", filename, err)
		}
	}
	key, err := x509.ParsePKCS1PrivateKey(der)
	if err == nil {
		return key, nil
	}
	log.Printf("cannot parse %q as PKCS1 key: %s", filename, err)

	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

type client struct {
	conn *ssh.ClientConn
}

func (c client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func NewClient(user, host, privateKeyFilename string) (client, error) {
	c := client{}
	var err error
	if c.conn, err = getClient(user, host, privateKeyFilename); err != nil {
		return c, err
	}
	return c, nil
}

// Execute Runs the command and returns a io.ReadCloser with the output
func (c client) Execute(command string) (out io.ReadCloser, err error) {
	session, err := c.conn.NewSession()
	if err != nil {
		return nil, fmt.Errorf("Execute: Failed to create session: %v", err)
	}
	defer func() {
		if err != nil {
			session.Close()
		}
	}()

	session.Stderr = os.Stderr
	p, err := session.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("Execute: error getting stdout: %v", err)
	}
	if err := session.Start(command); err != nil {
		return nil, fmt.Errorf("Execute: failed to run %q: %v", command, err)
	}
	return sessionOut{Session: session, Reader: p}, nil
}

type sessionOut struct {
	*ssh.Session
	io.Reader
}

func (c client) Cat(filename string) (out io.ReadCloser, err error) {
	//return c.Execute(fmt.Sprintf("/usr/bin/scp -qf %q", filename))
	return c.Execute(fmt.Sprintf("/usr/bin/cat %q", filename))
}

func (c client) Tail(filename string, lines int, follow bool) (io.ReadCloser, error) {
	f := ""
	if follow {
		f = "-f "
	}
	return c.Execute(fmt.Sprintf("/usr/bin/tail "+f+"-n %d %q", lines, filename))
}
