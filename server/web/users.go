package web

import (
	"fmt"
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/server/api"
	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	zk "github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

func getSaslUsers(p zookeeper.Permissions) ([]User, error) {
	users, err := zk.Users("", p)
	if err != nil {
		return []User{}, err
	}
	res := make([]User, len(users))
	for i, user := range users {
		res[i] = User{user, "SASL"}
	}
	return res, nil
}

func getCertUsers() ([]User, error) {
	truststore, err := api.GetKeystore("truststore")
	if err != nil {
		return []User{}, err
	}
	if truststore.Exists() {
		trusted, err := truststore.List()
		if err != nil {
			return []User{}, err
		}
		res := make([]User, len(trusted))
		for i, t := range trusted {
			dn, err := truststore.DistinguishedName(t.Alias)
			if err != nil {
				dn = ""
			}
			res[i] = User{t.Alias + dn, "Certificate"}
		}
		return res, nil
	}
	return []User{}, nil
}

func ListUsers(w http.ResponseWriter, r *http.Request) templates.Result {
	user := r.Context().Value("user").(mw.SessionUser)
	var (
		saslUsers []User
		certUsers []User
		err       error
	)
	saslUsers, err = getSaslUsers(user.Permissions)
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	cacheEntry, found := Cache.Get("certUsers")
	if !found {
		fmt.Println("no cache")
		certUsers, err = getCertUsers()
		if err != nil {
			return templates.ErrorRenderer(err)
		}
		Cache.Set("certUsers", certUsers, DefaultExpiration)
	} else {
		fmt.Println("cahe")
		certUsers = cacheEntry.([]User)
	}
	return templates.DefaultRenderer("users", append(saslUsers, certUsers...))
}
