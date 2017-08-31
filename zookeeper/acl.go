package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"fmt"
)

const (
	tPath string = "/kafka-acl/Topic"
	cPath string = "/kafka-acl/Cluster/kafka-cluster"
	gPath string = "/kafka-acl/Group"
)

type acl struct {
	Principal      string `json:"principal"`
	PermissionType string `json:"permissionType"`
	Operation      string `json:"operation"`
	Host           string `json:"host"`
}

type aclNode struct {
	Version int   `json:"version"`
	Acls    []acl `json:"acls"`
}

func ClusterAcl() ([]acl, error) {
	return aclFor(cPath)
}

func TopicAcl(t string) ([]acl, error) {
	return aclFor(fmt.Sprintf("%s/%s", tPath, t))
}

func GroupAcl(g string) ([]acl, error) {
	return aclFor(fmt.Sprintf("%s/%s", gPath, g))
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

func CreateAcl(principal, resource, resourceType string, perm Permission) error {
	operation := perm.String()
	if operation == "Read/Write" {
		operation = "All"
	}
	var (
		acls []acl
		err  error
	)
	path := "/kafka-acl/" + resourceType
	switch resourceType {
	case "Group":
		acls, err = GroupAcl(resource)
	case "Topic":
		acls, err = TopicAcl(resource)
	case "Cluster":
		acls, err = ClusterAcl()
		resource = "kafka-cluster"
	}
	path = fmt.Sprintf("%s/%s", path, resource)
	a := acl{Principal: "User:" + principal, PermissionType: "Allow", Operation: operation, Host: "*"}
	if err != nil && err != zk.ErrNoNode {
		return err
	}
	acls = append(acls, a)
	return setAcl(path, acls)
}

func setAcl(path string, acls []acl) error {
	data, err := json.Marshal(aclNode{
		Version: 1,
		Acls:    acls,
	})
	if err != nil {
		return err
	}
	ok, s, _ := conn.Exists(path)
	if ok {
		_, err = conn.Set(path, data, s.Version)
	} else {
		_, err = conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	}
	return err
}

func DeleteAcls(user string) error {
	permissions := PermissionsFor(user)
	for g, _ := range permissions.Groups {
		acls, _ := GroupAcl(g)
		setAcl(fmt.Sprintf("%s/%s", gPath, g), rejectAclFor(user, acls))
	}
	for t, _ := range permissions.Topics {
		acls, _ := TopicAcl(t)
		setAcl(fmt.Sprintf("%s/%s", tPath, t), rejectAclFor(user, acls))
	}
	acls, err := ClusterAcl()
	if err != nil {
		return err
	}
	return setAcl(cPath, rejectAclFor(user, acls))
}

func rejectAclFor(user string, acls []acl) []acl {
	var filtered []acl
	for _, a := range acls {
		if a.Principal != "User:"+user {
			filtered = append(filtered, a)
		}
	}
	return filtered
}
