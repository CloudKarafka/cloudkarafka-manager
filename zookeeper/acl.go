package zookeeper

import (
	"errors"

	"encoding/json"
	"fmt"
	"strings"

	"github.com/samuel/go-zookeeper/zk"
)

type AclPatternType int

const (
	LiteralPattern  AclPatternType = 0
	PrefixedPattern AclPatternType = 1
)

func AclPatternTypeFromString(v string) (AclPatternType, error) {
	switch strings.ToLower(v) {
	case "prefixed":
		return PrefixedPattern, nil
	case "literal":
		return LiteralPattern, nil
	}
	return -1, errors.New("Unknown pattern type")
}

type AclResourceType int

const (
	ClusterResource AclResourceType = 0
	TopicResource   AclResourceType = 1
	GroupResource   AclResourceType = 2
)

func (me AclResourceType) String() string {
	switch me {
	case ClusterResource:
		return "Cluster"
	case TopicResource:
		return "Topic"
	case GroupResource:
		return "Group"
	}
	return ""
}
func AclResourceFromString(v string) (AclResourceType, error) {
	switch strings.ToLower(v) {
	case "group":
		return GroupResource, nil
	case "topic":
		return TopicResource, nil
	case "cluster":
		return ClusterResource, nil
	}
	return -1, errors.New("Unknown resource type")
}

type AclRequest struct {
	PatternType    AclPatternType
	ResourceType   AclResourceType
	Name           string
	Principal      string
	Permission     string
	PermissionType string
	Host           string
}

func (me AclRequest) Path() string {
	if me.PatternType == PrefixedPattern {
		return fmt.Sprintf("/kafka-acl-extended/prefixed/%s/%s", me.ResourceType, me.Name)
	}
	return fmt.Sprintf("/kafka-acl/%s/%s", me.ResourceType, me.Name)
}
func (me AclRequest) Equal(acl map[string]string) bool {
	if me.Principal != acl["principal"] {
		return false
	}
	if me.Permission != acl["operation"] {
		return false
	}
	if me.PermissionType != acl["permissionType"] {
		return false
	}
	return true
}
func (me AclRequest) Data() map[string]string {
	host := me.Host
	if host == "" {
		host = "*"
	}
	return map[string]string{
		"host":           host,
		"principal":      me.Principal,
		"permissionType": strings.ToUpper(me.PermissionType),
		"operation":      strings.ToUpper(me.Permission)}
}

func CreateAcl(req AclRequest) error {
	if !Exists(req.Path()) {
		_, err := conn.Create(req.Path(), []byte("{\"version\": 1, \"acls\": []}"), 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			return err
		}
	}
	node, _, err := conn.Get(req.Path())
	if err != nil {
		return err
	}
	var a struct {
		Version int                 `json:"version"`
		Acls    []map[string]string `json:"acls"`
	}
	err = json.Unmarshal(node, &a)
	if err != nil {
		return err
	}
	for _, acl := range a.Acls {
		if req.Equal(acl) {
			fmt.Println("Already exists!!!")
			return nil
		}
	}
	return writeAcl(req.Path(), append(a.Acls, req.Data()))
}

func DeleteAcl(req AclRequest) error {
	node, _, err := conn.Get(req.Path())
	if err != nil {
		return err
	}
	var a struct {
		Version int                 `json:"version"`
		Acls    []map[string]string `json:"acls"`
	}
	if err = json.Unmarshal(node, &a); err != nil {
		return err
	}
	var n []map[string]string
	for _, acl := range a.Acls {
		if !req.Equal(acl) {
			n = append(n, acl)
		}
	}
	return writeAcl(req.Path(), n)
}

func writeAcl(path string, acls []map[string]string) error {
	data, err := json.Marshal(map[string]interface{}{
		"version": 1,
		"acls":    acls,
	})
	if err != nil {
		return err
	}
	ok, s, _ := conn.Exists(path)
	if ok && acls == nil {
		err = conn.Delete(path, s.Version)
	} else if ok {
		_, err = conn.Set(path, data, s.Version)
	} else {
		_, err = conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
	}
	if err != nil {
		return err
	}
	parts := strings.Split(path, "/")
	c := fmt.Sprintf("%s:%s", parts[len(parts)-2], parts[len(parts)-1])
	return createSeq("/kafka-acl-changes/acl_changes_", c)
}

func parseAclNode(basepath, child, resourceType, pattern string) (ACLRule, error) {
	path := fmt.Sprintf("%s/%s", basepath, child)
	if !Exists(path) {
		return ACLRule{}, nil
	}
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

func aclFromPath(basePath, resourceType, pattern string, pFn PermissionFunc) ([]ACLRule, error) {
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

func childAcls(resourceType string, permFn PermissionFunc) ([]ACLRule, error) {
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

func Acls(p Permissions) (ACLRules, error) {
	var a []ACLRule
	t, err := TopicAcls(p)
	if err != nil {
		return a, err
	}
	g, err := GroupAcls(p)
	if err != nil {
		return a, err
	}
	c, err := ClusterAcls(p)
	if err != nil {
		return a, err
	}
	a = make([]ACLRule, len(t)+len(g)+len(c))
	copy(a[0:], t)
	copy(a[len(t):], g)
	copy(a[len(t)+len(g):], c)
	return a, err
}

func TopicAcls(p Permissions) (ACLRules, error) {
	return childAcls("Topic", p.ReadTopic)
}

func GroupAcls(p Permissions) (ACLRules, error) {
	return childAcls("Group", p.ReadGroup)
}

func ClusterAcls(p Permissions) (ACLRules, error) {
	return childAcls("Cluster", p.ReadCluster)
}

func Acl(p Permissions, resourceType, name, patternType string) (ACLRule, error) {
	var (
		acls []ACLRule
		ar   ACLRule
		err  error
	)
	switch strings.ToLower(resourceType) {
	case "topic":
		acls, err = TopicAcls(p)
	case "group":
		acls, err = GroupAcls(p)
	case "cluster":
		acls, err = ClusterAcls(p)
	default:
		err = fmt.Errorf("Resource type must be one of; Topic, Group or Cluster, got %s", resourceType)
	}
	if err != nil {
		return ar, err
	}
	for _, r := range acls {
		if r.Resource.Name == name && strings.ToLower(r.Resource.PatternType) == strings.ToLower(patternType) {
			ar = r
			break
		}
	}
	return ar, err
}
