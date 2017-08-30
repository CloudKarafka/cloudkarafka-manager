package zookeeper

import (
	"fmt"
)

type Permission int

const (
	R  Permission = 1 << iota // R == 1 (iota has been reset)
	W  Permission = 1 << iota // W == 2
	RW Permission = R + W     // RW == 4
)

type Permissions struct {
	Username string
	Cluster  Permission
	Topics   map[string]Permission
	Groups   map[string]Permission
}

type permissionFunc func(string) bool

func (me Permissions) ClusterRead() bool {
	return (me.Cluster & R) == 1
}

func (me Permissions) ClusterWrite() bool {
	return (me.Cluster & W) == 1
}

func (me Permissions) TopicRead(t string) bool {
	return (me.Cluster&R) == 1 || (me.Topics[t]&R) == 1 || (me.Topics["*"]&R) == 1
}

func (me Permissions) TopicWrite(t string) bool {
	return (me.Cluster&W) == 1 || (me.Topics[t]&W) == 1 || (me.Topics["*"]&W) == 1
}

func (me Permissions) GroupRead(g string) bool {
	return (me.Cluster&R) == 1 || (me.Groups[g]&R) == 1 || (me.Groups["*"]&R) == 1
}

func (me Permissions) GroupWrite(g string) bool {
	return (me.Cluster&W) == 1 || (me.Groups[g]&W) == 1 || (me.Groups["*"]&W) == 1
}

func PermissionsFor(username string) Permissions {
	ar := Permissions{Username: username, Topics: make(map[string]Permission)}
	ca, _ := ClusterAcl()
	for _, a := range ca {
		if a.PermissionType != "Allow" {
			continue
		}
		if a.Principal == fmt.Sprintf("User:%s", username) {
			switch a.Operation {
			case "All":
				ar.Cluster = RW
			case "Read":
				ar.Cluster = R
			case "Write":
				ar.Cluster = W
			}
			break
		}
	}
	ar.Topics = permissionsMap(username, AllAcls(Topics, TopicAcl))
	ar.Groups = permissionsMap(username, AllAcls(Groups, GroupAcl))
	return ar
}

func permissionsMap(username string, aclMap map[string][]acl) map[string]Permission {
	m := make(map[string]Permission)
	for k, acls := range aclMap {
		for _, a := range acls {
			if a.PermissionType != "Allow" {
				continue
			}
			if a.Principal == fmt.Sprintf("User:%s", username) {
				switch a.Operation {
				case "All":
					m[k] = RW
				case "Read":
					m[k] = R
				case "Write":
					m[k] = W
				}
			}
		}
	}
	return m
}
