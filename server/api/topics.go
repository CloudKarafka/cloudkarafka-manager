package api

import (
	"github.com/84codes/cloudkarafka-mgmt/dm"
	"github.com/84codes/cloudkarafka-mgmt/zookeeper"

	"github.com/zenazn/goji/web"

	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

var (
	replicationFactorRequired = errors.New("ERROR: must suply replication_factor and it must be numeric.")
	partitionsRequired        = errors.New("ERROR: must suply partitions and it must be numeric")
)

func init() {
	Mux.Get("/topics", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		allTopics, err := zookeeper.Topics(p)
		var myTopics []string
		for _, t := range allTopics {
			if p.TopicRead(t) {
				myTopics = append(myTopics, t)
			}
		}
		if err != nil {
			internalError(w, err.Error())
		} else {
			WriteJson(w, myTopics)
		}
	})

	Mux.Post("/topics", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterWrite() {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprintf(w, "Insufficient privileges, requires cluster write.")
			return
		}
		t, err := decodeTopic(r)
		if err != nil {
			internalError(w, err.Error())
		} else {
			err := zookeeper.CreateTopic(t.Name, t.PartitionCount, t.ReplicationFactor, t.Config)
			if err != nil {
				internalError(w, err.Error())
				return
			}
			fmt.Printf("[INFO] action=create-topic user=%s topic=%s\n", p.Username, t.Name)
			getTopic(w, t.Name)
		}
	})

	Mux.Get("/topics/:name", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.TopicRead(c.URLParams["name"]) {
			Mux.NotFound(w)
			return
		}
		getTopic(w, c.URLParams["name"])
	})

	Mux.Put("/topics/:name", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterWrite() {
			Mux.NotFound(w)
			return
		}
		t, err := decodeTopic(r)
		if err != nil {
			internalError(w, err.Error())
			return
		}
		err = zookeeper.UpdateTopic(t.Name, t.PartitionCount, t.ReplicationFactor, t.Config)
		if err != nil {
			fmt.Println(err)
			internalError(w, err.Error())
			return
		}
		fmt.Printf("[INFO] action=update-topic user=%s topic=%s\n", p.Username, t.Name)
		getTopic(w, t.Name)
	})

	Mux.Delete("/topics/:name", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.ClusterWrite() {
			Mux.NotFound(w)
			return
		}
		err := zookeeper.DeleteTopic(c.URLParams["name"])
		if err != nil {
			internalError(w, err.Error())
			return
		}
		fmt.Printf("[INFO] action=delete-topic user=%s topic=%s\n", p.Username, c.URLParams["name"])
		w.WriteHeader(http.StatusNoContent)
	})

	Mux.Post("/topics/:name/spread", func(c web.C, w http.ResponseWriter, r *http.Request) {
		internalError(w, "Not yet implemented.")
		return
		/*	err := zookeeper.SpreadPartitionEvenly(c.URLParams["name"])
			if err != nil {
				internalError(w, err.Error())
			} else {
				fmt.Fprintf(w, string("Partition reassignment in progress"))
			}*/
	})

	Mux.Get("/topics/:name/throughput", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		topic := c.URLParams["name"]
		if !p.TopicRead(topic) {
			Mux.NotFound(w)
			return
		}
		in := dm.TopicBytesIn(topic)
		out := dm.TopicBytesOut(topic)
		WriteJson(w, map[string]dm.Series{"in": in, "out": out})
	})

	Mux.Get("/topics/:name/config", func(c web.C, w http.ResponseWriter, r *http.Request) {
		p := permissions(c)
		if !p.TopicRead(c.URLParams["name"]) {
			Mux.NotFound(w)
			return
		}
		cfg, err := zookeeper.TopicConfig(c.URLParams["name"])
		if err != nil {
			internalError(w, err.Error())
		} else {
			WriteJson(w, cfg)
		}
	})
}

func decodeTopic(r *http.Request) (dm.T, error) {
	var (
		t             dm.T
		err           error
		invalidConfig []string
	)
	switch r.Header.Get("content-type") {
	case "application/json":
		decoder := json.NewDecoder(r.Body)
		defer r.Body.Close()
		if err = decoder.Decode(&t); err != nil {
			return t, errors.New("Couldn't decode the json body in the request")
		}
	default:
		if err = r.ParseMultipartForm(512); err != nil {
			return t, errors.New("Couldn't parse the request body")
		}
		t.Name = r.PostForm.Get("name")
		t.PartitionCount, err = strconv.Atoi(r.PostForm.Get("partition_count"))
		t.ReplicationFactor, err = strconv.Atoi(r.PostForm.Get("replication_factor"))
		if r.PostForm.Get("config") != "" {
			rows := strings.Split(r.PostForm.Get("config"), "\n")
			cfg := make(map[string]interface{})
			for _, r := range rows {
				row := strings.TrimSpace(r)
				if row == "" {
					continue
				}
				cols := strings.Split(row, "=")
				if len(cols) != 2 {
					invalidConfig = append(invalidConfig, row)
					continue
				}
				key := strings.Trim(cols[0], " \n\r")
				val := strings.Trim(cols[1], " \n\r")
				cfg[key] = val
			}
			t.Config = cfg
		}
	}
	invalidConfig = append(invalidConfig, validateTopicConfig(t.Config)...)
	if len(invalidConfig) > 0 {
		return t, fmt.Errorf("Invalid config settings:<br />%s", strings.Join(invalidConfig, "<br />"))
	}
	return t, nil
}

var validConfigKeys = []string{
	"cleanup.policy",
	"compression.type",
	"delete.retention.ms",
	"file.delete.delay.ms",
	"flush.messages",
	"flush.ms",
	"follower.replication.throttled.replicas",
	"index.interval.bytes",
	"leader.replication.throttled.replicas",
	"max.message.bytes",
	"message.format.version",
	"message.timestamp.difference.max.ms",
	"message.timestamp.type",
	"min.cleanable.dirty.ratio",
	"min.compaction.lag.ms",
	"min.insync.replicas",
	"preallocate",
	"retention.bytes",
	"retention.ms",
	"segment.bytes",
	"segment.index.bytes",
	"segment.jitter.ms",
	"segment.ms",
	"unclean.leader.election.enable",
	"message.downconversion.enable",
}

func isValidConfigKey(key string) bool {
	for _, vck := range validConfigKeys {
		if key == vck {
			return true
		}
	}
	return false
}
func validateTopicConfig(cfg map[string]interface{}) []string {
	var errs []string
	for k, v := range cfg {
		if !isValidConfigKey(k) {
			errs = append(errs, fmt.Sprintf("%s=%s", k, v))
		}
	}
	return errs
}

func getTopic(w http.ResponseWriter, name string) {
	t, err := dm.Topic(name)
	if err != nil {
		Mux.NotFound(w)
		return
	}
	WriteJson(w, t)
}
