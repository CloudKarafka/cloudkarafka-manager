package zookeeper

import (
	"encoding/json"
)

// Correct permission for each operation is here: https://docs.confluent.io/current/kafka/authorization.html#acl-format

type PermissionFunc func(string) bool

type ACLResource struct {
	Name         string `json:"name"`
	ResourceType string `json:"resource_type"`
	PatternType  string `json:"pattern_type"`
}

type UserACL struct {
	Principal      string `json:"principal"`
	PermissionType string `json:"permission_type"`
	Operation      string `json:"operation"`
	Host           string `json:"host"`
}

type ACLRule struct {
	Resource ACLResource `json:"resource"`
	Users    []UserACL   `json:"users"`
}

type ACLRules []ACLRule

func (me ACLRules) Get(i int) interface{} {
	return me[i]
}

func (me ACLRules) Size() int {
	return len(me)
}

func (r ACLRule) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":          r.Resource.Name,
		"resource_type": r.Resource.ResourceType,
		"pattern_type":  r.Resource.PatternType,
		"users":         len(r.Users),
	})
}
