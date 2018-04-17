package zookeeper

import (
	"github.com/samuel/go-zookeeper/zk"

	"encoding/json"
	"fmt"
	"strings"
)

const (
	tPath string = "/kafka-acl/Topic"
	cPath string = "/kafka-acl/Cluster"
	gPath string = "/kafka-acl/Group"
)

var (
	UnknownResourceType error = fmt.Errorf("[ERROR] unknown resource type; known types are [User, Topic, Cluster]")
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

func ClusterAcl(name string) ([]acl, error) {
	return aclFor(fmt.Sprintf("%s/%s", cPath, name))
}

func TopicAcl(t string) ([]acl, error) {
	return aclFor(fmt.Sprintf("%s/%s", tPath, t))
}

func GroupAcl(g string) ([]acl, error) {
	return aclFor(fmt.Sprintf("%s/%s", gPath, g))
}

func ClusterAcls(_p Permissions) ([]string, error) {
	children, _, err := conn.Children(cPath)
	return children, err
}

func TopicsAcls(_p Permissions) ([]string, error) {
	children, _, err := conn.Children(tPath)
	return children, err
}

func GroupsAcls(_p Permissions) ([]string, error) {
	children, _, err := conn.Children(gPath)
	return children, err
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
	rows, _ := all(Permissions{
		Cluster: RW,
		Topics:  map[string]Permission{"*": RW},
		Groups:  map[string]Permission{"*": RW},
	})
	for _, r := range rows {
		acls[r], _ = details(r)
	}
	return acls
}

func CreateAcl(principal, name, resource, permissionType, host, perm string) error {
	if perm == "Read/Write" {
		perm = "All"
	}
	var (
		acls []acl
		err  error
		path string
	)
	switch resource {
	case "Group", "group":
		acls, err = GroupAcl(name)
		path = gPath
	case "Topic", "topic":
		acls, err = TopicAcl(name)
		path = tPath
	case "Cluster", "cluster":
		acls, err = ClusterAcl("kafka-cluster")
		name = "kafka-cluster"
		path = cPath
	default:
		return UnknownResourceType
	}
	if err != nil && err != zk.ErrNoNode {
		return err
	}
	if !strings.HasPrefix(principal, "User:") {
		principal = "User:" + principal
	}
	acls = append(acls, acl{Principal: principal,
		PermissionType: permissionType,
		Operation:      perm,
		Host:           host,
	})
	return setAcl(path, name, acls)
}

func setAcl(root, name string, acls []acl) error {
	path := fmt.Sprintf("%s/%s", root, name)
	data, err := json.Marshal(aclNode{
		Version: 1,
		Acls:    acls,
	})
	if err != nil {
		return err
	}
	ok, s, _ := conn.Exists(path)
	if ok && acls == nil {
		fmt.Println(path)
		err = conn.Delete(path, s.Version)
	} else if ok {
		_, err = conn.Set(path, data, s.Version)
	} else {
		_, err = conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	}
	if err != nil {
		return err
	}
	c := fmt.Sprintf("%s:%s", strings.Split(root, "/")[2], name)
	return change("/kafka-acl-changes/acl_changes_", []byte(c))
}

func DeleteAcl(user, resource, resourceType string) error {
	var (
		path string
		err  error
		acls []acl
	)
	switch resourceType {
	case "Group", "group":
		acls, err = GroupAcl(resource)
		if err != nil {
			return err
		}
		path = gPath
	case "Topic", "topic":
		fmt.Println("delete topic acl:", user, resource, resourceType)
		acls, err = TopicAcl(resource)
		if err != nil {
			return err
		}
		path = tPath
	case "Cluster", "cluster":
		acls, err = ClusterAcl("kafka-cluster")
		if err != nil {
			return err
		}
		path = cPath
	default:
		return UnknownResourceType
	}
	return setAcl(path, resource, rejectAclFor(user, acls))
}

func DeleteAcls(user string) error {
	permissions := PermissionsFor(user)
	for g, _ := range permissions.Groups {
		acls, _ := GroupAcl(g)
		setAcl(gPath, g, rejectAclFor(user, acls))
	}
	for t, _ := range permissions.Topics {
		acls, _ := TopicAcl(t)
		setAcl(tPath, t, rejectAclFor(user, acls))
	}
	acls, err := ClusterAcl("kafka-cluster")
	if err != nil {
		return err
	}
	return setAcl(cPath, "kafka-cluster", rejectAclFor(user, acls))
}

func rejectAclFor(user string, acls []acl) []acl {
	var filtered []acl
	for _, a := range acls {
		if a.Principal != user {
			filtered = append(filtered, a)
		}
	}
	return filtered
}
