package zookeeper

import (
	"fmt"
)

type Permission struct {
	Operation string
	Type      string
	Pattern   string
	Name      string
}

func (p Permission) Allow(resource string) bool {
	return p.Type == "ALLOW"
}
func (p Permission) Deny(resource string) bool {
	return p.Type == "DENY"
}
func (p Permission) All(resource string) bool {
	return p.Operation == "ALL"
}
func (p Permission) Alter(resource string) bool {
	return p.All(resource) || p.Operation == "ALTER"
}
func (p Permission) AlterConfigs(resource string) bool {
	return p.All(resource) || p.Operation == "ALTER_CONFIGS"
}
func (p Permission) Create(resource string) bool {
	return p.All(resource) || p.Operation == "CREATE"
}
func (p Permission) Read(resource string) bool {
	return p.All(resource) || p.Operation == "READ"
}
func (p Permission) Write(resource string) bool {
	return p.All(resource) || p.Operation == "WRITE"
}
func (p Permission) Delete(resource string) bool {
	return p.All(resource) || p.Operation == "DELETE"
}
func (p Permission) Describe(resource string) bool {
	return p.All(resource) || p.Operation == "DESCRIBE"
}
func (p Permission) DescribeConfigs(resource string) bool {
	return p.All(resource) || p.Operation == "DESCRIBE_CONFIGS"
}
func (p Permission) IdempotentWrite(resource string) bool {
	return p.All(resource) || p.Operation == "IDEMPOTENT_WRITE"
}

func PermissionsFor(username string) (Permissions, error) {
	cAcls, err := ClusterAcls(AdminPermissions)
	if err != nil {
		return Permissions{}, err
	}
	tAcls, err := TopicAcls(AdminPermissions)
	if err != nil {
		return Permissions{}, err
	}
	gAcls, err := GroupAcls(AdminPermissions)
	if err != nil {
		return Permissions{}, err
	}
	return Permissions{
		Cluster: permissionsMap(username, cAcls),
		Topic:   permissionsMap(username, tAcls),
		Group:   permissionsMap(username, gAcls)}, nil
}

func permissionsMap(username string, rules []ACLRule) []Permission {
	res := make([]Permission, 0)
	principal := fmt.Sprintf("User:%s", username)
	for _, rule := range rules {
		for _, user := range rule.Users {
			if user.Principal == principal {
				res = append(res, Permission{
					Operation: user.Operation,
					Type:      user.PermissionType,
					Pattern:   rule.Resource.PatternType,
					Name:      rule.Resource.Name})
			}
		}
	}
	return res
}
