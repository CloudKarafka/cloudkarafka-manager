package zookeeper

import "testing"

type spec struct {
	In       Permissions
	Expected bool
}

// { Operation, Type, Patter, Name }
func TestDescribeTopic(t *testing.T) {
	specs := []spec{
		{Permissions{Topic: []Permission{{"Describe", "Allow", "LITERAL", "*"}, {"Describe", "Deny", "LITERAL", "test"}}}, false},
		{Permissions{Topic: []Permission{{"Describe", "Allow", "LITERAL", "*"}, {"Describe", "Deny", "LITERAL", "test"}},
			Cluster: []Permission{{"Create", "Allow", "LITERAL", "kafka-cluster"}}}, true},
		{Permissions{Topic: []Permission{{"Describe", "Allow", "LITERAL", "test"}}}, true},
		{Permissions{Topic: []Permission{{"Describe", "Allow", "PREFIXED", "t"}}}, true},
		{Permissions{Topic: []Permission{{"Describe", "Allow", "LITERAL", "asdf"}}}, false},
		{Permissions{Topic: []Permission{{"Read", "Allow", "LITERAL", "*"}}}, false},
	}
	for _, spec := range specs {
		if spec.In.DescribeTopic("test") != spec.Expected {
			t.Errorf("FAILED! expected %v for Permission %v", spec.Expected, spec.In)
		}
	}
}

func TestCreateTopic(t *testing.T) {
	specs := []spec{
		{Permissions{Topic: []Permission{{"Create", "Allow", "LITERAL", "test"}}}, true},
	}
	for _, spec := range specs {
		if spec.In.CreateTopic("test") != spec.Expected {
			t.Errorf("FAILED! expected %v for Permission %v", spec.Expected, spec.In)
		}
	}
}

// { Operation, Type, Patter, Name }
func TestCreateAcl(t *testing.T) {
	specs := []spec{
		{Permissions{Cluster: []Permission{{"Alter", "Allow", "LITERAL", "kafka-cluster"}}}, true},
	}
	for _, spec := range specs {
		if spec.In.CreateAcl() != spec.Expected {
			t.Errorf("FAILED! expected %v for Permission %v", spec.Expected, spec.In)
		}
	}
}

// { Operation, Type, Patter, Name }
func TestListAcls(t *testing.T) {
	specs := []spec{
		{Permissions{Cluster: []Permission{{"Describe", "Deny", "LITERAL", "kafka-cluster"}},
			Topic: []Permission{{"Describe", "Allow", "LITERAL", "kafka-cluster"}}}, false},
		{Permissions{Cluster: []Permission{{"Describe", "Allow", "LITERAL", "kafka-cluster"}}}, true},
		{AdminPermissions, true},
	}
	for _, spec := range specs {
		if spec.In.ListAcls() != spec.Expected {
			t.Errorf("FAILED! expected %v for Permission %v", spec.Expected, spec.In)
		}
	}
}
