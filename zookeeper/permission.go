package zookeeper

import (
	"fmt"
	"strings"
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

// Check if rule matches resource on name
func (p Permission) Resource(resource string) bool {
	if p.Name == "*" {
		return true
	}
	if p.Pattern == "LITERAL" && p.Name == resource {
		return true
	}
	if p.Pattern == "PREFIXED" && strings.HasPrefix(resource, p.Name) {
		return true
	}
	return false
}
func (p Permission) check(op, resource string) bool {
	return (p.All(resource) || p.Operation == op) &&
		p.Resource(resource)
}
func (p Permission) Alter(resource string) bool {
	return p.check("ALTER", resource)
}
func (p Permission) AlterConfigs(resource string) bool {
	return p.check("ALTER_CONFIGS", resource)
}
func (p Permission) Create(resource string) bool {
	return p.check("CREATE", resource)
}
func (p Permission) Read(resource string) bool {
	return p.check("READ", resource)
}
func (p Permission) Write(resource string) bool {
	return p.check("WRITE", resource)
}
func (p Permission) Delete(resource string) bool {
	return p.check("DELETE", resource)
}
func (p Permission) Describe(resource string) bool {
	return p.check("DESCRIBE", resource)
}
func (p Permission) DescribeConfigs(resource string) bool {
	return p.check("DESCRIBE_CONFIGS", resource)
}
func (p Permission) IdempotentWrite(resource string) bool {
	return p.check("IDEMPOTENT_WRITE", resource)
}

func PermissionsFor(username string) (Permissions, error) {
	// Using AdminPermissions here since we need permissions to see all rules
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
