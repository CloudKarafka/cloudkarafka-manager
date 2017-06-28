package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"io"
)

var (
	path = "/kafka-acl/Topics/"
)

type acl struct {
	Principal      string `json:"principal"`
	PermissionType string `json:"permissionType"`
	Operation      string `json:"operation"`
	Host           string `json:"host"`
}

type aclNode struct {
	Version int   `json:version`
	Acls    []acl `json:"acls"`
}

func Acl(topic string) ([]acl, error) {
	node, _, err := conn.Get(path + topic)
	if err != nil {
		return nil, err
	}
	var a aclNode
	err = json.Unmarshal(node, &a)
	if err != nil {
		return nil, err
	}
	return a.Acls, nil
}

func CreateAcl(topic string, b io.Reader) error {
	acls, err := Acl(topic)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(b)
	var a acl
	err = decoder.Decode(&a)
	if err != nil {
		return err
	}
	n, err := json.Marshal(aclNode{
		Version: 1,
		Acls:    append(acls, a),
	})
	if err != nil {
		return err
	}
	_, err = conn.Create(path, n, 0, zk.WorldACL(zk.PermAll))
	return err
}
