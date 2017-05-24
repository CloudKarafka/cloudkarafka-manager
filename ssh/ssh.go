package ssh

import (
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"io/ioutil"
	"net"
	"os"
	"sync"
)

const (
	ZK_URL  = "127.0.0.1:2181"
	KFK_URL = "127.0.0.1:9092"
)

var (
	pool      map[string]*ssh.Client
	signer    ssh.Signer
	listeners map[string][2]net.Listener
	lock      = new(sync.Mutex)
)

func Forward(username, hostname string) (string, string) {
	if l, ok := listeners[hostname]; ok {
		return l[0].Addr().String(), l[1].Addr().String()
	}
	_, err := connect(username, hostname)
	printErr(err)

	port := len(listeners)*2 + 20000
	zk, err := net.Listen("tcp", fmt.Sprintf("localhost:%v", port))
	if err != nil {
		fmt.Println(err)
		return "", ""
	}
	go stream(zk, username, hostname, ZK_URL)
	kfk, err := net.Listen("tcp", fmt.Sprintf("localhost:%v", port+1))
	if err != nil {
		fmt.Println(err)
		return "", ""
	}
	go stream(kfk, username, hostname, KFK_URL)
	listeners[hostname] = [2]net.Listener{zk, kfk}

	return zk.Addr().String(), kfk.Addr().String()
}

func stream(l net.Listener, username, hostname, remoteUrl string) {
	for {
		local, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		client, err := connect(username, hostname)
		if err != nil {
			fmt.Println("[ERROR]", err)
			continue
		}
		remote, err := client.Dial("tcp", remoteUrl)
		if err != nil {
			fmt.Printf("[WARN] reconnecting reason=dial-failed hostname=%s remote-addr=%s forward-to=%s err=%s\n",
				hostname, client.RemoteAddr(), remoteUrl, err)
			removeFromPool(hostname)
			continue
		}
		go io.Copy(remote, local)
		go io.Copy(local, remote)
	}
}

func connect(username, hostname string) (*ssh.Client, error) {
	if c := pool[hostname]; c != nil {
		return c, nil
	}

	// An SSH client is represented with a ClientConn.
	//
	// To authenticate with the remote server you must pass at least one
	// implementation of AuthMethod via the Auth field in ClientConfig.
	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{
			// Use the PublicKeys method for remote authentication.
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
	}
	client, err := ssh.Dial("tcp", hostname+":22", config)
	if err != nil {
		return nil, err
	}
	lock.Lock()
	pool[hostname] = client
	lock.Unlock()
	return client, nil
}

func removeFromPool(hostname string) {
	lock.Lock()
	defer lock.Unlock()
	if conn, ok := pool[hostname]; ok {
		conn.Close()
	}
	delete(pool, hostname)
}

func exitOnError(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func printErr(err error) {
	if err != nil {
		fmt.Println(err)
	}
}

func init() {
	pool = make(map[string]*ssh.Client)
	listeners = make(map[string][2]net.Listener)
	// Create the Signer for this private key.

	password := os.Getenv("SSH_KEY_PASS")
	key, err := ioutil.ReadFile("certs/ssh.key")
	exitOnError(err)
	block, _ := pem.Decode(key)
	exitOnError(err)
	decrypted, err := x509.DecryptPEMBlock(block, []byte(password))
	exitOnError(err)
	rsakey, err := x509.ParsePKCS1PrivateKey(decrypted)
	exitOnError(err)
	signer, err = ssh.NewSignerFromKey(rsakey)
	exitOnError(err)
}
