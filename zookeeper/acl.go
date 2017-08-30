package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"io"
)

var (
	tPath = "/kafka-acl/Topic/"
	cPath = "/kafka-acl/Cluster/kafka-cluster"
	gPath = "/kafka-acl/Group"
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
	return aclFor(cPath)
}

func TopicAcl(t string) ([]acl, error) {
	return aclFor(tPath + t)
}

func GroupAcl(g string) ([]acl, error) {
	return aclFor(gPath + g)
}

func Groups(p Permissions) ([]string, error) {
	return all(gPath, p.GroupRead)
}

func aclFor(path string) ([]acl, error) {
	node, _, err := conn.Get(path)
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

type AclFunc func(string) ([]acl, error)

func AllAcls(all AllFunc, details AclFunc) map[string][]acl {
	acls := make(map[string][]acl)
	rows, _ := all(Permissions{Cluster: RW})
	for _, r := range rows {
		acls[r], _ = details(r)
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
