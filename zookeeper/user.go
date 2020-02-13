package zookeeper

import (
	"github.com/cloudkarafka/cloudkarafka-manager/server/auth"
	"github.com/samuel/go-zookeeper/zk"

	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var (
	userAlreadyExists = errors.New("User already exists")
)

type users []string

func (u users) Get(i int) interface{} {
	return u[i]
}

func (u users) TotalCount() int {
	return len(u)
}

func Users(username string, p Permissions) (users, error) {
	users, err := all("/config/users", func(usr string) bool {
		return p.ReadCluster("kafka-cluster") || usr == username
	})
	if err == zk.ErrNoNode {
		return []string{}, nil
	}
	return users, err
}

func User(name string) ([]byte, error) {
	user, _, err := conn.Get("/config/users/" + name)
	return user, err
}

func CreateUser(name, password string) error {
	cryptos := []string{"SCRAM-SHA-256"}
	cfg := make(map[string]string)
	for _, crypto := range cryptos {
		salt, storedKey, serverKey, itr := auth.CreateScramLogin(password, crypto)
		cfg[crypto] = fmt.Sprintf("salt=%s,stored_key=%s,server_key=%s,iterations=%v", salt, storedKey, serverKey, itr)
	}
	node := map[string]interface{}{
		"version": 1,
		"config":  cfg,
	}
	err := createPersistent("/config/users/"+name, node)
	if err == zk.ErrNodeExists {
		return userAlreadyExists
	}
	if err != nil {
		return err
	}
	return createSeq("/config/changes/config_change_", map[string]interface{}{
		"version":     2,
		"entity_path": "users/" + name,
	})
}

func userCredentials(name string) (string, string) {
	data, _, err := conn.Get(fmt.Sprintf("/config/users/%s", name))
	if err != nil {
		fmt.Println(err)
		return "", ""
	}
	node := make(map[string]interface{})
	err = json.Unmarshal(data, &node)
	if err != nil {
		fmt.Println(err)
		return "", ""
	}
	var salt, storedKey string
	cfg := node["config"].(map[string]interface{})
	sha256, ok := cfg["SCRAM-SHA-256"].(string)
	if !ok {
		return "", ""
	}
	for _, row := range strings.Split(sha256, ",") {
		if strings.HasPrefix(row, "salt=") {
			salt = strings.Replace(row, "salt=", "", 1)
		} else if strings.HasPrefix(row, "stored_key=") {
			storedKey = strings.Replace(row, "stored_key=", "", 1)
		}
		if salt != "" && storedKey != "" {
			break
		}
	}
	return salt, storedKey
}

func ValidateScramLogin(user, pass string) bool {
	enc := base64.StdEncoding.Strict()
	s, sk := userCredentials(user)
	salt, _ := enc.DecodeString(s)
	storedKey, _ := auth.CalculateSha256Keys(pass, salt)
	return storedKey == sk
}

func DeleteUser(name string) error {
	_, stats, _ := conn.Get("/config/users/" + name)
	err := conn.Delete("/config/users/"+name, stats.Version)
	if err != nil {
		return err
	}
	data := map[string]interface{}{
		"version":     2,
		"entity_path": "users/" + name,
	}
	return createSeq("/config/changes/config_change_", data)
}
