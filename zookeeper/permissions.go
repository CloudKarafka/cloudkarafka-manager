package zookeeper

type Permissions struct {
	Cluster []Permission
	Topic   []Permission
	Group   []Permission
}

func (p Permissions) DescribeAcls() bool {
	return p.check(p.Cluster, func(p Permission) bool {
		return p.Describe("kafka-cluster")
	})
}

func (p Permissions) check(perm []Permission, action func(Permission) bool) bool {
	allow := false
	for _, tp := range perm {
		if tp.Deny() && action(tp) {
			allow = false
			break
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

func (p Permissions) delete(perm []Permission, resource string) bool {
	return p.check(perm, func(p Permission) bool {
		return p.Delete(resource)
	})
}

func (p Permissions) WriteCluster(resource string) bool {
	return p.write(p.Cluster, resource)
}
func (p Permissions) ReadTopic(resource string) bool {
	return p.read(p.Topic, resource) || p.describe(p.Cluster, "kafka-cluster")
}
func (p Permissions) CreateTopic(resource string) bool {
	return p.create(p.Topic, resource) || p.create(p.Cluster, "kafka-cluster")
}
func (p Permissions) UpdateTopic(resource string) bool {
	return p.alter(p.Topic, resource)
}
func (p Permissions) ReadGroup(resource string) bool {
	return p.read(p.Group, resource) || p.describe(p.Cluster, "kafka-cluster")
}
func (p Permissions) ReadCluster(resource string) bool {
	return p.read(p.Cluster, resource)
}
func (p Permissions) DescribeTopic(resource string) bool {
	return p.describe(p.Topic, resource) || p.create(p.Cluster, "kafka-cluster")
}
func (p Permissions) DescribeGroup(resource string) bool {
	return p.describe(p.Group, resource)
}

func (p Permissions) AlterConfigsCluster() bool {
	return p.check(p.Cluster, func(p Permission) bool {
		return p.AlterConfigs("kafka-cluster")
	})
}
func (p Permissions) DeleteTopic(resource string) bool {
	return p.delete(p.Topic, resource) || p.alter(p.Cluster, "kafka-cluster")
}

func (p Permissions) DescribeConfigs() bool {
	return p.check(p.Cluster, func(p Permission) bool {
		return p.DescribeConfigs("kafka-cluster")
	})
}

func (p Permissions) CreateUser() bool {
	return p.create(p.Cluster, "kafka-cluster")
}
func (p Permissions) DeleteUser() bool {
	return p.delete(p.Cluster, "kafka-cluster")
}
func (p Permissions) CreateAcl() bool {
	return p.alter(p.Cluster, "kafka-cluster")
}
func (p Permissions) DeleteAcl() bool {
	return p.alter(p.Cluster, "kafka-cluster")
}
func (p Permissions) ListAcls() bool {
	return p.describe(p.Cluster, "kafka-cluster")
}

func (p Permissions) ListUsers() bool {
	return p.DescribeConfigs()
}

func (p Permissions) ListBrokers() bool {
	return p.DescribeConfigs()
}

func (p Permissions) ListGroups() bool {
	return p.describe(p.Cluster, "kafka-cluster")
}

var AllowAll = []Permission{{"All", "Allow", "LITERAL", "*"}}
var AdminPermissions = Permissions{
	Cluster: AllowAll,
	Topic:   AllowAll,
	Group:   AllowAll,
}
