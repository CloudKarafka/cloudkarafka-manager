package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Partition struct {
	Leader          int            `json:"leader"`
	Replicas        []int          `json:"replicas"`
	ISR             []int          `json:"isr"`
	LeaderEpoch     int            `json:"leader_epoch"`
	Version         int            `json:"version"`
	ControllerEpoch int            `json:"controller_epoch"`
	Metrics         map[string]int `json:"metrics"`
}

type TopicConfig struct {
	Version int                    `json:"version"`
	Data    map[string]interface{} `json:"config"`
}

func (t TopicConfig) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Data)
}

type Topic struct {
	Name       string
	Partitions []Partition
	Config     TopicConfig
	Deleted    bool
	Metrics    map[string]int
}

func (t Topic) Size() int {
	sum := 0
	for _, p := range t.Partitions {
		sum += p.Metrics["Size"]
	}
	return sum
}
func (t Topic) Messages() int {
	sum := 0
	for _, p := range t.Partitions {
		sum += p.Metrics["LogEndOffset"] - p.Metrics["LogStartOffset"]
	}
	return sum
}

func (t Topic) MarshalJSON() ([]byte, error) {
	res := map[string]interface{}{
		"name":       t.Name,
		"deleted":    t.Deleted,
		"partitions": t.Partitions,
	}
	if len(t.Metrics) > 0 {
		res["metrics"] = t.Metrics
	}
	if len(t.Config.Data) > 0 {
		res["config"] = t.Config
	}
	if v := t.Size(); v != 0 {
		res["size"] = v
	}
	if v := t.Messages(); v != 0 {
		res["message_count"] = v
	}
	return json.Marshal(res)
}

type TopicResponse struct {
	Topic Topic
	Error error
}

type TopicRequest struct {
	TopicNames []string
	Config     bool
	Metrics    []MetricRequest
}

func fetchTopic(ctx context.Context, topicName string) (Topic, error) {
	topic, err := zookeeper.Topic(topicName)
	if err != nil {
		if err == zookeeper.PathDoesNotExistsErr {
			fmt.Fprintf(os.Stderr, "[INFO] FetchTopic: topic %s does not exists in zookeeper", topicName)
		} else {
			fmt.Fprintf(os.Stderr, "[INFO] FetchTopic: %s", err)
		}
		return Topic{}, err
	}
	t := Topic{
		Name:       topicName,
		Partitions: make([]Partition, len(topic.Partitions)),
		Metrics:    make(map[string]int),
	}
	for p, replicas := range topic.Partitions {
		var par Partition
		partitionPath := fmt.Sprintf("/brokers/topics/%s/partitions/%s/state", topicName, p)
		if err := zookeeper.Get(partitionPath, &par); err == nil {
			i, _ := strconv.Atoi(p)
			par.Replicas = replicas
			par.Metrics = make(map[string]int)
			t.Partitions[i] = par
		}
	}
	return t, nil
}

func fetchTopicMetrics(ctx context.Context, metrics []MetricRequest) (map[string][]Metric, error) {
	res := make(map[string][]Metric)
	ch := GetMetricsAsync(metrics)
	l := len(metrics)
	for i := 0; i < l; i++ {
		select {
		case response := <-ch:
			if response.Error != nil {
				return res, response.Error
			} else {
				for _, metric := range response.Metrics {
					res[metric.Topic] = append(res[metric.Topic], metric)
				}
			}
		case <-ctx.Done():
			return res, fmt.Errorf("Fetching partition metrics failed: %s", ctx.Err())
		}
	}
	return res, nil
}

func fetchConfig(ctx context.Context, topicName string) (TopicConfig, error) {
	var topicConfig TopicConfig
	err := zookeeper.Get(fmt.Sprintf("/config/topics/%s", topicName), &topicConfig)
	return topicConfig, err
}

func FetchTopics(ctx context.Context, topicNames []string, config bool, metricReqs []MetricRequest) ([]TopicResponse, error) {
	deletedTopics := zookeeper.TopicsMarkedForDeletion()
	res := make([]TopicResponse, len(topicNames))
	var (
		err     error
		metrics map[string][]Metric
	)
	if len(metricReqs) > 0 {
		if metrics, err = fetchTopicMetrics(ctx, metricReqs); err != nil {
			return nil, err
		}
	}
	for i, topicName := range topicNames {
		topic, err := fetchTopic(ctx, topicName)
		res[i] = TopicResponse{}
		if err != nil {
			res[i].Error = fmt.Errorf("Failed to fetch info for topic %s from Zookeeper: %s", topicName, err)
		} else {
			for _, dt := range deletedTopics {
				if dt == topic.Name {
					topic.Deleted = true
				}
			}
			res[i].Topic = topic
			if config {
				cfg, err := fetchConfig(ctx, topic.Name)
				if err != nil {
					res[i].Error = fmt.Errorf("Failed to fetch config for topic %s: %s", topicName, err)
				} else {
					res[i].Topic.Config = cfg
				}
			}
			if len(metricReqs) > 0 {
				for _, metric := range metrics[topic.Name] {
					value := int(metric.Value)
					switch metric.Type {
					case "Log":
						i, err := strconv.Atoi(metric.Partition)
						if err != nil {
							res[i].Error = fmt.Errorf("Failed to parse metric response: %s", err)
						} else {
							if topic.Partitions[i].Leader == metric.Broker {
								topic.Partitions[i].Metrics[metric.Name] = value
							}
						}
					case "BrokerTopicMetrics":
						topic.Metrics[metric.Name] += value
					default:
						return nil, fmt.Errorf("Unhandled metrics response %s/%s", metric.Type, metric.Name)
					}
				}
			}
		}
	}
	return res, nil
}

func FetchTopic(ctx context.Context, topicName string, config bool, metricReqs []MetricRequest) (Topic, error) {
	r, err := FetchTopics(ctx, []string{topicName}, config, metricReqs)
	if err != nil {
		return Topic{}, err
	}
	if len(r) == 0 {
		return Topic{}, fmt.Errorf("Topic %s not found", topicName)
	}
	if r[0].Error != nil {
		return Topic{}, r[0].Error
	}
	return r[0].Topic, nil
}

func CreateTopic(ctx context.Context, name string, partitions, replicationFactor int, topicConfig map[string]string) error {
	a, err := adminClient()
	if err != nil {
		log.Error("create_topic", log.ErrorEntry{err})
		return err
	}
	results, err := a.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             name,
			NumPartitions:     partitions,
			ReplicationFactor: replicationFactor,
			Config:            topicConfig}},
		kafka.SetAdminOperationTimeout(15*time.Second))
	if err != nil {
		log.Error("create_topic", log.ErrorEntry{err})
		return err
	}
	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError {
			log.Error("create_topic", log.ErrorEntry{r.Error})
			return r.Error
		}
	}
	return nil
}

func UpdateTopicConfig(ctx context.Context, name string, topicConfig map[string]interface{}) error {
	changes := make([]kafka.ConfigEntry, 0)
	for k, v := range topicConfig {
		changes = append(changes, kafka.ConfigEntry{
			Name:      k,
			Value:     v.(string),
			Operation: kafka.AlterOperationSet})
	}
	a, err := adminClient()
	if err != nil {
		log.Error("update_topic_config", log.ErrorEntry{err})
		return err
	}
	configResource := kafka.ConfigResource{
		Type:   kafka.ResourceTopic,
		Name:   name,
		Config: changes,
	}
	results, err := a.AlterConfigs(ctx,
		[]kafka.ConfigResource{configResource},
		kafka.SetAdminRequestTimeout(30*time.Second))
	if err != nil {
		log.Error("update_topic_config", log.ErrorEntry{err})
		return err
	}
	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError {
			log.Error("update_topic_config", log.ErrorEntry{r.Error})
			return r.Error
		}
	}
	return nil
}

func AddParitions(ctx context.Context, name string, increaseTo int) error {
	a, err := adminClient()
	if err != nil {
		log.Error("update_topic_config", log.ErrorEntry{err})
		return err
	}
	spec := kafka.PartitionsSpecification{
		Topic:      name,
		IncreaseTo: increaseTo}
	results, err := a.CreatePartitions(ctx,
		[]kafka.PartitionsSpecification{spec},
		kafka.SetAdminRequestTimeout(15*time.Second))
	if err != nil {
		log.Error("add_partitions", log.ErrorEntry{err})
		return err
	}
	for _, r := range results {
		if r.Error.Code() != kafka.ErrNoError {
			log.Error("add_partitions", log.ErrorEntry{r.Error})
			return r.Error
		}
	}
	return nil
}

func DeleteTopic(ctx context.Context, name string) error {
	a, err := adminClient()
	results, err := a.DeleteTopics(ctx, []string{name},
		kafka.SetAdminOperationTimeout(15*time.Second))
	if err != nil {
		log.Error("delete_topic", log.ErrorEntry{err})
		return err
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			log.Error("delete_topic", log.ErrorEntry{result.Error})
			return result.Error
		}
	}
	return nil
}
