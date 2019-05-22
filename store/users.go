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

func getCertUsers() ([]KafkaUser, error) {
	truststore, err := GetKeystore("truststore")
	if err != nil {
		return noKafkaUsers, err
	}
	if truststore.Exists() {
		trusted, err := truststore.List()
		if err != nil {
			return noKafkaUsers, err
		}
		res := make([]KafkaUser, len(trusted))
		for i, t := range trusted {
			dn, err := truststore.DistinguishedName(t.Alias)
			if err != nil {
				dn = ""
			}
			res[i] = KafkaUser{t.Alias + dn, "Certificate"}
		}
		return res, nil
	}
	return noKafkaUsers, err
}

func Users(p zookeeper.Permissions) ([]KafkaUser, error) {
	var (
		saslUsers []KafkaUser
		certUsers []KafkaUser
		err       error
	)
	saslUsers, err = getSaslUsers(p)
	if err != nil {
		return noKafkaUsers, err
	}
	certUsers, _ = getCertUsers()
	return append(saslUsers, certUsers...), nil
}
