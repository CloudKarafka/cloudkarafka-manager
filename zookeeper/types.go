package zookeeper

// Correct permission for each operation is here: https://docs.confluent.io/current/kafka/authorization.html#acl-format

type permissionFunc func(string) bool

type ACLResource struct {
	Name         string
	ResourceType string
	PatternType  string
}

type UserACL struct {
	Principal      string
	PermissionType string
	Operation      string
	Host           string
}

type ACLRule struct {
	Resource ACLResource
	Users    []UserACL
}

type Permissions struct {
	Cluster []Permission
	Topic   []Permission
	Group   []Permission
}

func (p Permissions) DescribeAcls() bool {
	r := "kafka-cluster"
	return p.check(p.Cluster, r, func(p Permission) bool {
		return p.Describe(r)
	})
}

func (p Permissions) check(perm []Permission, resource string, action func(Permission) bool) bool {
	allow := false
	for _, tp := range perm {
		if tp.Deny(resource) && action(tp) {
			return false
		}
		if tp.Allow(resource) && action(tp) {
			allow = true
		}
	}
	return allow
}
func (p Permissions) read(perm []Permission, resource string) bool {
	return p.check(perm, resource, func(p Permission) bool {
		return p.Read(resource)
	})
}
func (p Permissions) write(perm []Permission, resource string) bool {
	return p.check(perm, resource, func(p Permission) bool {
		return p.Write(resource)
	})
}
func (p Permissions) create(perm []Permission, resource string) bool {
	return p.check(perm, resource, func(p Permission) bool {
		return p.Create(resource)
	})
}
func (p Permissions) alter(perm []Permission, resource string) bool {
	return p.check(perm, resource, func(p Permission) bool {
		return p.Alter(resource)
	})
}
func (p Permissions) WriteCluster(resource string) bool {
	return p.write(p.Cluster, resource)
}
func (p Permissions) ReadTopic(resource string) bool {
	return p.read(p.Topic, resource)
}
func (p Permissions) CreateTopic(resource string) bool {
	return p.create(p.Topic, resource)
}
func (p Permissions) UpdateTopic(resource string) bool {
	return p.alter(p.Topic, resource)
}
func (p Permissions) ReadGroup(resource string) bool {
	return p.read(p.Group, resource)
}
func (p Permissions) ReadCluster(resource string) bool {
	return p.read(p.Cluster, resource)
}

func (p Permissions) DeleteTopic(resource string) bool {
	return p.check(p.Topic, resource, func(p Permission) bool {
		return p.Delete(resource)
	})
}

func (p Permissions) ListUsers() bool {
	return true
}
func (p Permissions) ListTopics() bool {
	return true
}
func (p Permissions) CreateUser() bool {
	resource := "kafka-cluster"
	return p.check(p.Cluster, resource, func(p Permission) bool {
		return p.Write(resource)
	})
}
func (p Permissions) DeleteUser() bool {
	resource := "kafka-cluster"
	return p.check(p.Cluster, resource, func(p Permission) bool {
		return p.Delete(resource)
	})
}
func (p Permissions) CreateAcl() bool {
	resource := "kafka-cluster"
	return p.check(p.Cluster, resource, func(p Permission) bool {
		return p.Alter(resource)
	})
}
func (p Permissions) DeleteAcl() bool {
	resource := "kafka-cluster"
	return p.check(p.Cluster, resource, func(p Permission) bool {
		return p.Alter(resource)
	})
}

var AllowAll = []Permission{Permission{"ALL", "ALLOW", "LITERAL", "*"}}
var AdminPermissions = Permissions{
	Cluster: AllowAll,
	Topic:   AllowAll,
	Group:   AllowAll,
}
