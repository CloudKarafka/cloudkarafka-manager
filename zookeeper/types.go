package zookeeper

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
	allow := false
	r := "kafka-cluster"
	for _, p := range p.Cluster {
		if p.Describe(r) && p.Deny(r) {
			return false
		}
		if p.Describe(r) && p.Allow(r) {
			allow = true
		}
	}
	return allow
}

func (p Permissions) read(perm []Permission, resource string) bool {
	allow := false
	for _, tp := range perm {
		if tp.Deny(resource) && tp.Read(resource) {
			return false
		}
		if tp.Allow(resource) && tp.Read(resource) {
			allow = true
		}
	}
	return allow
}
func (p Permissions) write(perm []Permission, resource string) bool {
	allow := false
	for _, tp := range perm {
		if tp.Deny(resource) && tp.Write(resource) {
			return false
		}
		if tp.Allow(resource) && tp.Write(resource) {
			allow = true
		}
	}
	return allow
}
func (p Permissions) ReadCluster() bool {
	return p.read(p.Cluster, "kafka-cluster")
}

func (p Permissions) WriteCluster() bool {
	return p.write(p.Cluster, "kafka-cluster")
}

func (p Permissions) ReadTopic(resource string) bool {
	return p.read(p.Topic, resource)
}
func (p Permissions) ReadGroup(resource string) bool {
	return p.read(p.Group, resource)
}

func (p Permissions) DeleteTopic(resource string) bool {
	allow := false
	for _, tp := range p.Topic {
		if tp.Deny(resource) && tp.Delete(resource) {
			return false
		}
		if tp.Allow(resource) && tp.Delete(resource) {
			allow = true
		}
	}
	return allow
}
func (p Permissions) ListUsers() bool {
	return true
}

func (p Permissions) CreateUser() bool {
	allow := false
	r := "kafka-cluster"
	for _, p := range p.Cluster {
		if p.Write(r) && p.Deny(r) {
			return false
		}
		if p.Write(r) && p.Allow(r) {
			allow = true
		}
	}
	return allow
}
func (p Permissions) DeleteUser() bool {
	allow := false
	r := "kafka-cluster"
	for _, p := range p.Cluster {
		if p.Delete(r) && p.Deny(r) {
			return false
		}
		if p.Delete(r) && p.Allow(r) {
			allow = true
		}
	}
	return allow
}

var AllowAll = []Permission{Permission{"ALL", "ALLOW", "LITERAL", "*"}}
var AdminPermissions = Permissions{
	Cluster: AllowAll,
	Topic:   AllowAll,
	Group:   AllowAll,
}
