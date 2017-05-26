package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"fmt"
	"net"
	"time"
)

var (
	conn *zk.Conn
)

func Start() error {
	err := connect("localhost:2181")
	return err
}

func Stop() {
	if conn != nil {
		conn.Close()
	}
}

func connect(url string) error {
	var err error
	if conn != nil {
		conn.Close()
	}
	opts := zk.WithDialer(func(network, address string, timeout time.Duration) (net.Conn, error) {
		fmt.Println("net", network, "addr", address)
		return net.DialTimeout(network, address, timeout)
	})
	conn, _, err = zk.Connect([]string{url}, 30*time.Second, opts)
	if err != nil {
		time.Sleep(1 * time.Second)
		connect(url)
		return err
	}
	fmt.Printf("[INFO] %v zookeeper shovel connected\n", url)

	return nil
}
