package api

import (
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"

	"github.com/zenazn/goji/web"

	"encoding/json"
	"net/http"
)

type user struct {
	Name, Password string
}

func init() {
	Mux.Get("/whoami", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		WriteJson(w, p)
	})

	Mux.Get("/users", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterRead() {
			http.NotFound(w, r)
			return
		}
		users, err := zookeeper.Users(p)
		if err != nil {
			internalError(w, err.Error())
			return
		}
		WriteJson(w, users)
	})

	Mux.Post("/users", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		u, err := decodeUser(r)
		if err != nil {
			internalError(w, err.Error())
			return
		}
		createUser(w, u)
		err = zookeeper.CreateUser(u.Name, u.Password)
		if err != nil {
			internalError(w, err.Error())
		}
	})

	Mux.Get("/users/:name", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterRead() && c.URLParams["name"] != p.Username {
			http.NotFound(w, r)
			return
		}
		user := zookeeper.PermissionsFor(c.URLParams["name"])
		WriteJson(w, user)
	})

	Mux.Delete("/users/:name", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterWrite() {
			http.NotFound(w, r)
			return
		}
		zookeeper.DeleteUser(c.URLParams["name"])
		w.WriteHeader(http.StatusNoContent)
	})
}

func decodeUser(r *http.Request) (user, error) {
	var (
		u   user
		err error
	)
	switch r.Header.Get("content-type") {
	case "application/json":
		decoder := json.NewDecoder(r.Body)
		err = decoder.Decode(&u)
	default:
		err = r.ParseMultipartForm(512)
		u = user{Name: r.PostFormValue("name"), Password: r.PostFormValue("password")}
	}
	return u, err
}

func createUser(w http.ResponseWriter, u user) {
}
