package zookeeper

type Permissions struct {
	Cluster []Permission
	Topic   []Permission
	Group   []Permission
}

func (p Permissions) DescribeAcls() bool {
	r := "kafka-cluster"
	return p.check(p.Cluster, func(p Permission) bool {
		return p.Describe(r)
	})
}

func (p Permissions) check(perm []Permission, action func(Permission) bool) bool {
	allow := false
	for _, tp := range perm {
		if tp.Deny() && action(tp) {
			return false
		}
		if tp.Allow() && action(tp) {
			allow = true
		}
	}
	return allow
}
func (p Permissions) read(perm []Permission, resource string) bool {
	return p.check(perm, func(p Permission) bool {
		return p.Read(resource)
	})
}
func (p Permissions) write(perm []Permission, resource string) bool {
	return p.check(perm, func(p Permission) bool {
		return p.Write(resource)
	})
}
func (p Permissions) create(perm []Permission, resource string) bool {
	return p.check(perm, func(p Permission) bool {
		return p.Create(resource)
	})
}
func (p Permissions) alter(perm []Permission, resource string) bool {
	return p.check(perm, func(p Permission) bool {
		return p.Alter(resource)
	})
}
func (p Permissions) describe(perm []Permission, resource string) bool {
	return p.check(perm, func(p Permission) bool {
		return p.Describe(resource)
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
func (p Permissions) DescribeTopic(resource string) bool {
	return p.describe(p.Topic, resource) || p.create(p.Cluster, "kafka-cluster")
}

func (p Permissions) DeleteTopic(resource string) bool {
	return p.check(p.Topic, func(p Permission) bool {
		return p.Delete(resource)
	})
}

func (p Permissions) DescribeConfigs() bool {
	return p.check(p.Cluster, func(p Permission) bool {
		return p.DescribeConfigs("kafka-cluster")
	})
}

func (p Permissions) CreateUser() bool {
	return p.check(p.Cluster, func(p Permission) bool {
		return p.Write("kafka-cluster")
	})
}
func (p Permissions) DeleteUser() bool {
	return p.check(p.Cluster, func(p Permission) bool {
		return p.Delete("kafka-cluster")
	})
}
func (p Permissions) CreateAcl() bool {
	return p.check(p.Cluster, func(p Permission) bool {
		return p.Alter("kafka-cluster")
	})
}
func (p Permissions) DeleteAcl() bool {
	return p.check(p.Cluster, func(p Permission) bool {
		return p.Alter("kafka-cluster")
	})
}

func (p Permissions) ListAcls() bool {
	return p.check(p.Cluster, func(p Permission) bool {
		return p.Describe("kafka-cluster")
	})
}
func (p Permissions) ListTopics() bool {
	return p.DescribeTopic("*")
}

func (p Permissions) ListUsers() bool {
	return p.DescribeConfigs()
}

var AllowAll = []Permission{Permission{"ALL", "ALLOW", "LITERAL", "*"}}
var AdminPermissions = Permissions{
	Cluster: AllowAll,
	Topic:   AllowAll,
	Group:   AllowAll,
}
