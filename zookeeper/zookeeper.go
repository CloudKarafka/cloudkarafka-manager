package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"fmt"
	"net"
	"time"
)

func Start() error {
	conn, err := connect("localhost:2181")
	if err != nil {
		return err
	}
	go clusterInfo(conn)
	//go consumerOffset(cluster)
	return nil
}

func connect(url string) (*zk.Conn, error) {
	opts := zk.WithDialer(func(network, address string, timeout time.Duration) (net.Conn, error) {
		fmt.Println("net", network, "addr", address)
		return net.DialTimeout(network, address, timeout)
	})
	conn, _, err := zk.Connect([]string{url}, 30*time.Second, opts)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[INFO] %v zookeeper shovel connected\n", url)

	return conn, nil
}
