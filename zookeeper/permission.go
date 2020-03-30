package zookeeper

import (
	"fmt"
	"strings"
)

type Permission struct {
	Operation string
	Type      string
	Pattern   string
	Principal string
}

func (p Permission) check(op, resource string) bool {
	return (p.All() || p.Operation == op) && p.Resource(resource)
}

// Check if rule matches resource on name
func (p Permission) Resource(principal string) bool {
	allowed := false
	if p.Principal == "*" {
		allowed = true
	} else if p.Pattern == "literal" && p.Principal == principal {
		allowed = true
	} else if p.Pattern == "prefixed" && strings.HasPrefix(principal, p.Principal) {
		allowed = true
	}
	return allowed
}

func (p Permission) Allow() bool {
	return strings.ToLower(p.Type) == "allow"
}
func (p Permission) Deny() bool {
	return strings.ToLower(p.Type) == "deny"
}
func (p Permission) All() bool {
	return strings.ToLower(p.Operation) == "all"
}
func (p Permission) Alter(resource string) bool {
	return p.check("Alter", resource)
}
func (p Permission) AlterConfigs(resource string) bool {
	return p.check("AlterConfigs", resource)
}
func (p Permission) Create(resource string) bool {
	return p.check("Create", resource)
}
func (p Permission) Read(resource string) bool {
	return p.check("Read", resource)
}
func (p Permission) Write(resource string) bool {
	return p.check("Write", resource)
}
func (p Permission) Delete(resource string) bool {
	return p.check("Delete", resource)
}
func (p Permission) Describe(resource string) bool {
	return p.check("Describe", resource)
}
func (p Permission) DescribeConfigs(resource string) bool {
	return p.check("DescribeConfigs", resource)
}
func (p Permission) IdempotentWrite(resource string) bool {
	return p.check("IdempotentWrite", resource)
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
					Principal: rule.Resource.Name})
			}
		}
	}
	return res
}
