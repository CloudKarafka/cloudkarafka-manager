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
}

func (me Permissions) ClusterRead() bool {
	return (me.Cluster & R) == 1
}

func (me Permissions) ClusterWrite() bool {
	return (me.Cluster & W) == 1
}

func (me Permissions) TopicRead(t string) bool {
	return (me.Cluster&R) == 1 || (me.Topics[t]&R) == 1
}

func (me Permissions) TopicWrite(t string) bool {
	return (me.Cluster&W) == 1 || (me.Topics[t]&W) == 1
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
	for t, acls := range AllTopicAcls() {
		for _, a := range acls {

			if a.PermissionType != "Allow" {
				continue
			}
			if a.Principal == fmt.Sprintf("User:%s", username) {
				switch a.Operation {
				case "All":
					ar.Topics[t] = RW
				case "Read":
					ar.Topics[t] = R
				case "Write":
					ar.Topics[t] = W
				}
			}
		}
	}
	return ar
}
