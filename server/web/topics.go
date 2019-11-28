package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/server/cookie"
	mw "github.com/cloudkarafka/cloudkarafka-manager/server/middleware"
	"github.com/cloudkarafka/cloudkarafka-manager/store"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"goji.io/pat"
)

func filterTopics(topics []string, query string) []string {
	if query == "" {
		return topics
	}
	res := make([]string, len(topics))
	i := 0
	for _, topic := range topics {
		if strings.Contains(topic, query) {
			res[i] = topic
			i += 1
		}
	}
	return res[:i]
}

func ListTopics(w http.ResponseWriter, r *http.Request) templates.Result {
	user := r.Context().Value("user").(mw.SessionUser)
	queryParams := r.URL.Query()
	topicNames, err := zookeeper.Topics(user.Permissions)
	if err != nil {
		log.Error("list_topics", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	if len(queryParams["s"]) > 0 {
		topicNames = filterTopics(topicNames, queryParams["s"][0])
	}
	metricRequests := make([]store.MetricRequest, len(config.BrokerUrls)*3)
	i := 0
	for id, _ := range config.BrokerUrls {
		metricRequests[i] = store.MetricRequest{id, store.BeanAllTopicsBytesInPerSec, "OneMinuteRate"}
		metricRequests[i+1] = store.MetricRequest{id, store.BeanAllTopicsBytesOutPerSec, "OneMinuteRate"}
		metricRequests[i+2] = store.MetricRequest{id, store.BeanAllTopicsLogSize, "Value"}
		i = i + 3
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topics, err := store.FetchTopics(ctx, topicNames, false, metricRequests)
	if err != nil {
		log.Error("list_topics", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Topic.Name < topics[j].Topic.Name
	})
	return templates.DefaultRenderer("topics", topics)
}

func ViewTopic(w http.ResponseWriter, r *http.Request) templates.Result {
	user := r.Context().Value("user").(mw.SessionUser)
	name := pat.Param(r, "name")
	fmt.Println(user)
	if !user.Permissions.DescribeTopic(name) {
		return templates.ErrorRenderer(errors.New("You don't have permissions to view this topic."))
	}
	metricRequests := make([]store.MetricRequest, len(config.BrokerUrls)*5)
	i := 0
	for id, _ := range config.BrokerUrls {
		metricRequests[i] = store.MetricRequest{id, store.BeanTopicBytesInPerSec(name), "OneMinuteRate"}
		metricRequests[i+1] = store.MetricRequest{id, store.BeanTopicBytesOutPerSec(name), "OneMinuteRate"}
		metricRequests[i+2] = store.MetricRequest{id, store.BeanTopicLogSize(name), "Value"}
		metricRequests[i+3] = store.MetricRequest{id, store.BeanTopicLogStart(name), "Value"}
		metricRequests[i+4] = store.MetricRequest{id, store.BeanTopicLogEnd(name), "Value"}
		i = i + 5
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topic, err := store.FetchTopic(ctx, name, true, metricRequests)
	if err != nil {
		log.Error("view_topic", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	return templates.DefaultRenderer("topic", topic)
}

var topicConf = []TopicSetting{
	TopicSetting{"cleanup.policy", "list", []interface{}{"compact", "delete"}, "delete"},
	TopicSetting{"min.insync.replicas", "int", []interface{}{}, 1},
	TopicSetting{"retention.bytes", "long", []interface{}{}, -1},
	TopicSetting{"retention.ms", "long", []interface{}{}, 604800000},
	TopicSetting{"segment.bytes", "int", []interface{}{}, 1073741824},
	TopicSetting{"max.message.bytes", "int", []interface{}{}, 1000012},
	TopicSetting{"compression.type", "string", []interface{}{"uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"}, "producer"},
	TopicSetting{"delete.retention.ms", "long", []interface{}{}, 86400000},
	TopicSetting{"file.delete.delay.ms", "long", []interface{}{}, 60000},
	TopicSetting{"flush.messages", "long", []interface{}{}, 9223372036854775807},
	TopicSetting{"flush.ms", "long", []interface{}{}, 9223372036854775807},
	TopicSetting{"preallocate", "boolean", []interface{}{}, false},
	TopicSetting{"index.interval.bytes", "int", []interface{}{}, 4096},
	TopicSetting{"message.timestamp.difference.max.ms", "long", []interface{}{}, 9223372036854775807},
	TopicSetting{"message.timestamp.type", "string", []interface{}{"CreateTime", "LogAppendTime"}, "CreateTime"},
	TopicSetting{"min.cleanable.dirty.ratio", "double", []interface{}{}, 0.5},
	TopicSetting{"min.compaction.lag.ms", "long", []interface{}{}, 0},
	TopicSetting{"segment.index.bytes", "int", []interface{}{}, 10485760},
	TopicSetting{"segment.jitter.ms", "long", []interface{}{}, 0},
	TopicSetting{"segment.ms", "long", []interface{}{}, 604800000},
	TopicSetting{"unclean.leader.election.enable", "boolean", []interface{}{}, false},
}

func CreateTopic(w http.ResponseWriter, r *http.Request) templates.Result {
	return templates.DefaultRenderer(
		"create_topic",
		TopicForm{topicConf, []string{}, map[string]interface{}{}})
}
func SaveTopic(w http.ResponseWriter, r *http.Request) templates.Result {
	r.ParseForm()
	formValues := r.Form
	topicForm := TopicForm{
		ConfigOptions: topicConf,
		Errors:        make([]string, 0)}
	topicForm.LoadValues(formValues)

	var (
		name              string
		numParts          int
		replicationFactor int
		conf              = make(map[string]string)
		err               error
	)

	name = formValues.Get("name")
	if name == "" {
		topicForm.Errors = append(topicForm.Errors, "You need to give the topic a name")
	}
	formValues.Del("name")

	if len(name) <= 0 || len(name) > 255 {
		topicForm.Errors = append(topicForm.Errors, "Topic name must be between 0 and 255 characters")
	}
	if match, _ := regexp.MatchString("\\A[A-Za-z0-9\\.\\-_]+\\z", name); !match {
		topicForm.Errors = append(topicForm.Errors, "Topic name can only contain letters, numbers and dot, underline and strike (., _, -).")
	}

	partitions := formValues.Get("partitions")
	if partitions == "" {
		topicForm.Errors = append(topicForm.Errors, "You need to specify number of partitions for the topic.")
	} else {
		numParts, err = strconv.Atoi(partitions)
		if err != nil {
			topicForm.Errors = append(topicForm.Errors, "Partition count must be a number")
		}
	}
	formValues.Del("partitions")

	reps := formValues.Get("replication_factor")
	if reps == "" {
		topicForm.Errors = append(topicForm.Errors, "You need to specify replication factor for the topic.")
	} else {
		replicationFactor, err = strconv.Atoi(reps)
		if err != nil {
			topicForm.Errors = append(topicForm.Errors, "Replication factor must be a number")
		}
	}
	formValues.Del("replication_factor")

	for k, _ := range formValues {
		v := formValues.Get(k)
		if v != "" {
			conf[k] = v
		}
	}

	if len(topicForm.Errors) > 0 {
		return templates.DefaultRenderer("create_topic", topicForm)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	err = store.CreateTopic(ctx, name, numParts, replicationFactor, conf)
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	http.Redirect(w, r, fmt.Sprintf("/topics/%s", url.QueryEscape(name)), 302)
	return nil
}

func DeleteTopic(w http.ResponseWriter, r *http.Request) {
	user := r.Context().Value("user").(mw.SessionUser)
	name := pat.Param(r, "name")
	if !user.Permissions.DeleteTopic(name) {
		http.Redirect(w, r, fmt.Sprintf("/topics/%s", url.QueryEscape(name)), 302)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	var redirUrl string
	session, _ := cookie.Cookiestore.Get(r, "session")
	err := store.DeleteTopic(ctx, name)
	if err != nil {
		redirUrl = fmt.Sprintf("/topics/%s", url.QueryEscape(name))
		session.AddFlash(err.Error(), "flash_danger")
	} else {
		redirUrl = "/topics"
		session.AddFlash(fmt.Sprintf("Topic %s marked for deletion", name), "flash_info")
	}
	session.Save(r, w)
	http.Redirect(w, r, redirUrl, 302)
}

func EditTopic(w http.ResponseWriter, r *http.Request) templates.Result {
	user := r.Context().Value("user").(mw.SessionUser)
	name := pat.Param(r, "name")
	if !user.Permissions.ReadTopic(name) {
		return templates.ErrorRenderer(errors.New("You don't have permissions to view this topic."))
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topic, err := store.FetchTopic(ctx, name, true, nil)
	if err != nil {
		log.Error("edit_topic", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	values := make(map[string]interface{})
	values["name"] = topic.Name
	values["partitions"] = len(topic.Partitions)
	values["replication_factor"] = len(topic.Partitions[0].Replicas)
	for k, v := range topic.Config.Data {
		values[k] = v.(string)
	}
	return templates.DefaultRenderer("edit_topic", TopicForm{topicConf, []string{}, values})
}

func UpdateTopicConfig(w http.ResponseWriter, r *http.Request) templates.Result {
	name := pat.Param(r, "name")
	r.ParseForm()
	formValues := r.Form
	topicForm := TopicForm{
		ConfigOptions: topicConf,
		Errors:        make([]string, 0)}
	topicForm.LoadValues(formValues)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	topicConfig := make(map[string]interface{})
	for k, _ := range formValues {
		v := formValues.Get(k)
		if v != "" {
			topicConfig[k] = v
		}
	}
	err := store.UpdateTopicConfig(ctx, name, topicConfig)
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	http.Redirect(w, r, fmt.Sprintf("/topics/%s", url.QueryEscape(name)), 302)
	return nil
}

func AddTopicPartitions(w http.ResponseWriter, r *http.Request) templates.Result {
	name := pat.Param(r, "name")
	r.ParseForm()
	count, err := strconv.Atoi(r.FormValue("count"))
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	err = store.AddParitions(ctx, name, count)
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	http.Redirect(w, r, fmt.Sprintf("/topics/%s", url.QueryEscape(name)), 302)
	return nil
}
