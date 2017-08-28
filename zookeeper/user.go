package zookeeper

import (
	"cloudkarafka-mgmt/server/auth"

	"github.com/samuel/go-zookeeper/zk"

	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

func Users() ([]string, error) {
	users, _, err := conn.Children("/config/users")
	return users, err
}

func User(name string) ([]byte, error) {
	user, _, err := conn.Get("/config/users/" + name)
	return user, err
}

func CreateUser(name, password string) error {
	salt, storedKey, serverKey, itr := auth.CreateScramLogin(name, password)
	scramSha512 := fmt.Sprintf("salt=%s,stored_key=%s,server_key=%s,iterations=%v", salt, storedKey, serverKey, itr)
	node := map[string]interface{}{"version": 1, "config": map[string]string{"SCRAM-SHA-512": scramSha512}}
	config, err := json.Marshal(node)
	if err != nil {
		return nil
	}
	_, err = conn.Create("/config/users/"+name, config, 0, zk.WorldACL(zk.PermAll))
	return err
}

func UserCredentials(name string) (string, string) {
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
	for _, row := range strings.Split(cfg["SCRAM-SHA-512"].(string), ",") {
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
	s, sk := UserCredentials(user)
	salt, _ := enc.DecodeString(s)
	clientKey := enc.EncodeToString(auth.CalculateKey([]byte(pass), []byte("Client Key"), salt, 4096))
	return clientKey == sk
}
