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
	m "github.com/cloudkarafka/cloudkarafka-manager/metrics"
	"github.com/cloudkarafka/cloudkarafka-manager/templates"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"goji.io/pat"
)

func sumMetrics(l []m.TopicMetricValue) int {
	res := 0
	for _, v := range l {
		res += v.Value
	}
	return res
}
func addMetrics(ctx context.Context, topics []m.Topic) ([]Topic, error) {
	metricNames := []string{"BytesOutPerSec", "BytesInPerSec"}
	metrics, err := m.FetchTopicMetrics(ctx, "*", metricNames)
	if err != nil {
		return nil, err
	}
	res := make([]Topic, len(topics))
	for i, topic := range topics {
		res[i] = Topic{topic, m.TopicConfig{}, -1, -1}
		if v, ok := metrics[topic.Name]; ok {
			for k, v := range v {
				if k == "BytesInPerSec" {
					res[i].BytesIn = sumMetrics(v)
				} else if k == "BytesOutPerSec" {
					res[i].BytesOut = sumMetrics(v)
				}
			}
		}
	}
	return res, nil
}

func ListTopics(w http.ResponseWriter, r *http.Request) templates.Result {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topics, err := m.FetchTopicList(ctx, p)
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	topicsWithMetrics, err := addMetrics(ctx, topics)
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	sort.Slice(topicsWithMetrics, func(i, j int) bool {
		return topicsWithMetrics[i].Name < topicsWithMetrics[j].Name
	})

	return templates.DefaultRenderer("topics", topicsWithMetrics)
}

func ViewTopic(w http.ResponseWriter, r *http.Request) templates.Result {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	name := pat.Param(r, "name")
	if !p.ReadTopic(name) {
		return templates.ErrorRenderer(errors.New("You don't have permissions to view this topic."))
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topic, err := m.FetchTopic(ctx, name)
	if err != nil {
		if err == zookeeper.PathDoesNotExistsErr {
			return templates.ErrorRenderer(errors.New("This topic doesn't exists."))
		}
		return templates.ErrorRenderer(err)
	}
	config, err := m.FetchTopicConfig(ctx, name)
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	return templates.DefaultRenderer("topic", Topic{topic, config, -1, 1})
}

var topicConf = []TopicConfig{
	TopicConfig{"cleanup.policy", "list", []string{"compact", "delete"}, "delete"},
	TopicConfig{"min.insync.replicas", "int", []string{}, 1},
	TopicConfig{"retention.bytes", "long", []string{}, -1},
	TopicConfig{"retention.ms", "long", []string{}, 604800000},
	TopicConfig{"segment.bytes", "int", []string{}, 1073741824},
	TopicConfig{"max.message.bytes", "int", []string{}, 1000012},

	TopicConfig{"compression.type", "string", []string{"uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"}, "producer"},
	TopicConfig{"delete.retention.ms", "long", []string{}, 86400000},
	TopicConfig{"file.delete.delay.ms", "long", []string{}, 60000},
	TopicConfig{"flush.messages", "long", []string{}, 9223372036854775807},
	TopicConfig{"flush.ms", "long", []string{}, 9223372036854775807},
	TopicConfig{"preallocate", "boolean", []string{}, false},
	TopicConfig{"index.interval.bytes", "int", []string{}, 4096},
	TopicConfig{"message.timestamp.difference.max.ms", "long", []string{}, 9223372036854775807},
	TopicConfig{"message.timestamp.type", "string", []string{"CreateTime", "LogAppendTime"}, "CreateTime"},
	TopicConfig{"min.cleanable.dirty.ratio", "double", []string{}, 0.5},
	TopicConfig{"min.compaction.lag.ms", "long", []string{}, 0},
	TopicConfig{"segment.index.bytes", "int", []string{}, 10485760},
	TopicConfig{"segment.jitter.ms", "long", []string{}, 0},
	TopicConfig{"segment.ms", "long", []string{}, 604800000},
	TopicConfig{"unclean.leader.election.enable", "boolean", []string{}, false},
}

func CreateTopic(w http.ResponseWriter, r *http.Request) templates.Result {
	return templates.DefaultRenderer(
		"create_topic",
		TopicForm{topicConf, []string{}, map[string]string{}})
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

	adminConfig := &kafka.ConfigMap{"bootstrap.servers": strings.Join(config.BrokerUrls.List(), ",")}
	a, err := kafka.NewAdminClient(adminConfig)
	if err != nil {
		log.Error("create_topic", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	results, err := a.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             name,
			NumPartitions:     numParts,
			ReplicationFactor: replicationFactor,
			Config:            conf}},
		kafka.SetAdminOperationTimeout(5*time.Second))
	if err != nil {
		log.Error("create_topic", log.ErrorEntry{err})
		return templates.ErrorRenderer(err)
	}
	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError {
			log.Error("create_topic", log.ErrorEntry{r.Error})
			return templates.ErrorRenderer(r.Error)
		}
	}
	http.Redirect(w, r, fmt.Sprintf("/topics/%s", url.QueryEscape(name)), 302)
	return nil
}

func DeleteTopic(w http.ResponseWriter, r *http.Request) {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	name := pat.Param(r, "name")
	if !p.DeleteTopic(name) {
		http.Redirect(w, r, fmt.Sprintf("/topics", url.QueryEscape(name)), 302)
		return

	}

	adminConfig := &kafka.ConfigMap{"bootstrap.servers": strings.Join(config.BrokerUrls.List(), ",")}
	a, err := kafka.NewAdminClient(adminConfig)
	if err != nil {
		log.Error("delete_topic", log.ErrorEntry{err})
		http.Redirect(w, r, fmt.Sprintf("/topics", url.QueryEscape(name)), 302)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	results, err := a.DeleteTopics(
		ctx,
		[]string{name},
		kafka.SetAdminOperationTimeout(5*time.Second))
	if err != nil {
		log.Error("delete_topic", log.ErrorEntry{err})
		http.Redirect(w, r, fmt.Sprintf("/topics", url.QueryEscape(name)), 302)
		return
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			log.Error("delete_topic", log.ErrorEntry{result.Error})
			http.Redirect(w, r, fmt.Sprintf("/topics", url.QueryEscape(name)), 302)
			return
		}
	}
	http.Redirect(w, r, fmt.Sprintf("/topics", url.QueryEscape(name)), 302)
}

func EditTopic(w http.ResponseWriter, r *http.Request) templates.Result {
	p := r.Context().Value("permissions").(zookeeper.Permissions)
	name := pat.Param(r, "name")
	if !p.ReadTopic(name) {
		return templates.ErrorRenderer(errors.New("You don't have permissions to view this topic."))
	}
	ctx, cancel := context.WithTimeout(r.Context(), config.JMXRequestTimeout)
	defer cancel()
	topic, err := m.FetchTopic(ctx, name)
	if err != nil {
		return templates.ErrorRenderer(err)
	}
	return templates.DefaultRenderer("edit_topic", topic)
}

func UpdateTopic(w http.ResponseWriter, r *http.Request) templates.Result {
	return nil
}
