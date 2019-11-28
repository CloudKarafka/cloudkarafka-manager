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
