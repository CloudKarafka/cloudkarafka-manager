package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"io"
)

var (
	tPath = "/kafka-acl/Topics/"
	cPath = "/kafka-acl/Cluster/kafka-cluster"
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

func ClusterAcl() ([]acl, error) {
	node, _, err := conn.Get(cPath)
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

func TopicAcl(topic string) ([]acl, error) {
	node, _, err := conn.Get(tPath + topic)
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

func AllTopicAcls() map[string][]acl {
	acls := make(map[string][]acl)
	topics, _ := Topics(Permissions{Cluster: RW})
	for _, t := range topics {
		acls[t], _ = TopicAcl(t)
	}
	return acls
}

func CreateAcl(topic string, b io.Reader) error {
	acls, err := TopicAcl(topic)
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
	_, err = conn.Create(tPath, n, 0, zk.WorldACL(zk.PermAll))
	return err
}
