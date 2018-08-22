package api

/*
import (
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

func StatsOverview(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	data := store.IndexedNames("type")
	WriteJson(w, data)
}

func Stats(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	vars := mux.Vars(r)
	metric := vars["metric"]
	data := store.
		SelectWithIndex(metric).
		GroupBy(func(d store.Data) string {
			return d.Key()
		})
	d := make([]store.Data, len(data))
	for _, v := range data {
		d = append(d, v.Sort().Last())
	}
	WriteJson(w, d)
}

func promKey(d store.Data) string {
	keys := make([]string, len(d.Tags))
	i := 0
	for _, k := range d.Tags {
		keys[i] = k
		i++
	}
	return fmt.Sprintf("cloudkarafka_%s_%s_%s_%s{measure=\"%s\"}",
		strings.Replace(d.Tags["domain"], ".", "", -1),
		d.Tags["type"],
		d.Tags["name"],
		d.Tags["request"],
		d.Tags["measure"])
}

func StatsPrometheus(w http.ResponseWriter, r *http.Request, s zookeeper.Permissions) {
	fmt.Println("Prom request", mux.Vars(r))
	for _, metric := range store.IndexedNames("type") {
		data := store.
			SelectWithIndex(metric).
			GroupBy(func(d store.Data) string {
				return d.Key()
			})
		for _, v := range data {
			d := v.Sort().Last()
			fmt.Fprintf(w, "%s %v\n", promKey(d), d.Value)
		}
	}
}*/
