package zookeeper

import (
	"errors"

	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"net"
	"time"
)

var (
	conn                 *zk.Conn
	PathDoesNotExistsErr = errors.New("Zookeeper: Path does not exists")
)

type AllFunc func(Permissions) ([]string, error)

func Stop() {
	if conn != nil {
		conn.Close()
	}
}

func Connect(urls []string) error {
	var err error
	if conn != nil {
		conn.Close()
	}
	opts := zk.WithDialer(func(network, address string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout(network, address, timeout)
	})
	conn, _, err = zk.Connect(urls, 30*time.Second, opts)
	if err != nil {
		return err
	}
	go watchBrokers()
	go watchTopics()
	return nil
}

func Exists(path string) bool {
	exists, _, _ := conn.Exists(path)
	return exists
}

func WatchChildren(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	return conn.ChildrenW(path)
}

func all(path string, fn permissionFunc) ([]string, error) {
	rows := make([]string, 0)
	if exists, _, _ := conn.Exists(path); !exists {
		return rows, PathDoesNotExistsErr
	}
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

func Get(path string, v interface{}) error {
	return get(path, v)
}

func get(path string, v interface{}) error {
	if exists, _, _ := conn.Exists(path); !exists {
		return PathDoesNotExistsErr // fmt.Errorf("Zookeeper: Path \"%s\" doesn't exists", path)
	}
	data, _, err := conn.Get(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

func createSeq(path string, data interface{}) error {
	return create(path, data, zk.FlagSequence)
}

func createPersistent(path string, data interface{}) error {
	return create(path, data, 0)
}

func create(path string, data interface{}, flag int) error {
	var (
		bytes []byte
		err   error
	)
	if str, ok := data.(string); ok {
		bytes = []byte(str)
	} else {
		bytes, err = json.Marshal(data)
	}
	if err != nil {
		return err
	}
	_, err = conn.Create(path, bytes, int32(flag), zk.WorldACL(zk.PermAll))
	return err
}

func set(path string, data interface{}) error {
	_, stat, err := conn.Exists(path)
	if err != nil {
		return err
	}
	enc, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = conn.Set(path, enc, stat.Version)
	return err
}
