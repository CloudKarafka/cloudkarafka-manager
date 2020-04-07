package store

import "github.com/cloudkarafka/cloudkarafka-manager/zookeeper"

type KafkaUser struct {
	Name string
	Type string
}

var noKafkaUsers = []KafkaUser{}

func getSaslUsers(p zookeeper.Permissions) ([]KafkaUser, error) {
	users, err := zookeeper.Users("", p)
	if err != nil {
		return noKafkaUsers, err
	}
	res := make([]KafkaUser, len(users))
	for i, user := range users {
		res[i] = KafkaUser{user, "SASL"}
	}
	return res, nil
}

func Users(p zookeeper.Permissions) ([]KafkaUser, error) {
	var (
		saslUsers []KafkaUser
		err       error
	)
	saslUsers, err = getSaslUsers(p)
	if err != nil {
		return noKafkaUsers, err
	}
	return saslUsers, nil
}
