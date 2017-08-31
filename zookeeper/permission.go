package zookeeper

import (
	"encoding/json"
	"fmt"
)

type Permission int

const (
	N  Permission = 0
	R  Permission = 1
	W  Permission = 2
	RW Permission = R + W // RW == 3
)

func (p *Permission) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*p = ParsePermission(s)
	return nil
}

func ParsePermission(s string) Permission {
	switch s {
	default:
		return N
	case "Read":
		return R
	case "Write":
		return W
	case "Read/Write":
		return RW
	}
}

func (p Permission) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.String())
}

func (p Permission) String() string {
	var s string
	switch {
	case p >= RW:
		s = "Read/Write"
	case p >= W:
		s = "Write"
	case p >= R:
		s = "Read"
	default:
		s = "None"
	}
	return s
}

type Permissions struct {
	Username string                `json:"username"`
	Cluster  Permission            `json:"cluster"`
	Topics   map[string]Permission `json:"topics"`
	Groups   map[string]Permission `json:"groups"`
}

type permissionFunc func(string) bool

func (me Permissions) ClusterRead() bool {
	return me.Cluster >= R
}

func (me Permissions) ClusterWrite() bool {
	return me.Cluster >= W
}

func (me Permissions) TopicRead(t string) bool {
	return me.Cluster >= R || me.Topics[t] >= R || me.Topics["*"] >= R
}

func (me Permissions) TopicWrite(t string) bool {
	return me.Cluster >= W || me.Topics[t] >= W || me.Topics["*"] >= W
}

func (me Permissions) GroupRead(g string) bool {
	return me.Cluster >= R || me.Groups[g] >= R || me.Groups["*"] >= R
}

func (me Permissions) GroupWrite(g string) bool {
	return me.Cluster >= W || me.Groups[g] >= W || me.Groups["*"] >= W
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
