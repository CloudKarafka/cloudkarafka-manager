package zookeeper

import (
	"errors"

	"encoding/json"
	"fmt"
	"strings"
)

func CreateAcl(principal, name, resource, permissionType, host, perm string) error {
	return nil
	/*
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
	*/
}

/*
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
	return createSeq("/kafka-acl-changes/acl_changes_", c)
}
*/
func DeleteAcl(user, resource, resourceType string) error {
	return nil
	/*
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
	*/
}

func DeleteAcls(user string) error {
	return nil
	/*
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
	*/
}

/*
func rejectAclFor(user string, acls []acl) []acl {
	var filtered []acl
	for _, a := range acls {
		if a.Principal != user {
			filtered = append(filtered, a)
		}
	}
	return filtered
}
*/
func parseAclNode(basepath, child, resourceType, pattern string) (ACLRule, error) {
	path := fmt.Sprintf("%s/%s", basepath, child)
	node, _, err := conn.Get(path)
	if err != nil {
		return ACLRule{}, err
	}
	var a struct {
		Version int                 `json:"version"`
		Acls    []map[string]string `json:"acls"`
	}
	err = json.Unmarshal(node, &a)
	if err != nil {
		return ACLRule{}, err
	}
	rule := ACLRule{
		Resource: ACLResource{
			Name:         child,
			ResourceType: strings.ToUpper(resourceType),
			PatternType:  pattern},
		Users: []UserACL{},
	}
	for _, acl := range a.Acls {
		rule.Users = append(rule.Users, UserACL{
			Principal:      acl["principal"],
			PermissionType: acl["permissionType"],
			Operation:      acl["operation"],
			Host:           acl["host"]})
	}
	return rule, nil
}

func aclFromPath(basePath, resourceType, pattern string, pFn permissionFunc) ([]ACLRule, error) {
	var res []ACLRule
	path := fmt.Sprintf("%s/%s", basePath, resourceType)
	if !Exists(path) {
		return res, nil
	}
	children, _, err := conn.Children(path)
	if err != nil {
		return res, err
	}
	for _, child := range children {
		if pFn(child) {
			node, err := parseAclNode(path, child, resourceType, pattern)
			if err != nil {
				return res, err
			}
			res = append(res, node)
		}
	}
	return res, nil
}

func childAcls(resourceType string, permFn permissionFunc) ([]ACLRule, error) {
	var res []ACLRule
	acls, err := aclFromPath("/kafka-acl", resourceType, "LITERAL", permFn)
	if err != nil {
		return res, err
	}
	res = append(res, acls...)
	acls, err = aclFromPath("/kafka-acl-extended/prefixed", resourceType, "PREFIXED", permFn)
	if err != nil {
		return res, err
	}
	res = append(res, acls...)
	return res, nil
}

func TopicAcls(p Permissions) ([]ACLRule, error) {
	if !p.DescribeAcls() {
		return []ACLRule{}, errors.New("Not authorized")
	}
	return childAcls("Topic", p.ReadTopic)
}

func GroupAcls(p Permissions) ([]ACLRule, error) {
	if !p.DescribeAcls() {
		return []ACLRule{}, errors.New("Not authorized")
	}
	return childAcls("Group", p.ReadGroup)
}

func ClusterAcls(p Permissions) (ACLRule, error) {
	if !p.DescribeAcls() {
		return ACLRule{}, errors.New("Not authorized")
	}
	res, err := parseAclNode("/kafka-acl/Cluster", "kafka-cluster", "Cluster", "LITERAL")
	if err != nil {
		return res, err
	}
	return res, nil
}
