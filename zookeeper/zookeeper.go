package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"fmt"
	"net"
	"time"
)

var (
	conn                *zk.Conn
	authenticaionMethod string
)

type AllFunc func(Permissions) ([]string, error)

func Start() error {
	err := connect("localhost:2181")
	return err
}

func Stop() {
	if conn != nil {
		conn.Close()
	}
}

func SetAuthentication(method string) {
	authenticaionMethod = method
}
func SkipAuthentication() bool {
	return authenticaionMethod == "none"
}
func SkipAuthenticationWithWrite() bool {
	return authenticaionMethod == "none-with-write"
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

func all(path string, fn permissionFunc) ([]string, error) {
	rows := make([]string, 0)
	children, _, err := conn.Children(path)
	if err != nil {
		return rows, err
	}
	for _, c := range children {
		if fn(c) {
			rows = append(rows, c)
		}
	}
	return rows, nil
}

func get(path string, v interface{}) error {
	data, _, err := conn.Get(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func change(path string, data []byte) error {
	_, err := conn.Create(path, []byte(data), zk.FlagSequence, zk.WorldACL(zk.PermAll))
	return err
}
