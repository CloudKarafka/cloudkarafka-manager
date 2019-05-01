package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
)

type Partition struct {
	Leader          int   `json:"leader"`
	Replicas        []int `json:"replicas"`
	ISR             []int `json:"isr"`
	LeaderEpoch     int   `json:"leader_epoch"`
	Version         int   `json:"version"`
	ControllerEpoch int   `json:"controller_epoch"`
	Metrics         map[string]int
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
		"name":               t.Name,
		"deleted":            t.Deleted,
		"partitions":         t.Partitions,
		"metrics":            t.Metrics,
		"partition_count":    len(t.Partitions),
		"replication_factor": 0,
	}
	if len(t.Config.Data) > 0 {
		res["config"] = t.Config
	}
	if len(t.Partitions) > 0 {
		res["replication_factor"] = len(t.Partitions[0].Replicas)
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
	ch := QueryBrokerAsync(metrics)
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

func FetchTopics(ctx context.Context, req TopicRequest) ([]TopicResponse, error) {
	deletedTopics := zookeeper.TopicsMarkedForDeletion()
	res := make([]TopicResponse, len(req.TopicNames))
	var (
		err     error
		metrics map[string][]Metric
	)
	if len(req.Metrics) > 0 {
		if metrics, err = fetchTopicMetrics(ctx, req.Metrics); err != nil {
			return nil, err
		}
	}
	for i, topicName := range req.TopicNames {
		topic, err := fetchTopic(ctx, topicName)
		res[i] = TopicResponse{}
		if err != nil {
			res[i].Error = err
		} else {
			for _, dt := range deletedTopics {
				if dt == topic.Name {
					topic.Deleted = true
				}
			}
			res[i].Topic = topic
			if req.Config {
				cfg, err := fetchConfig(ctx, topic.Name)
				if err != nil {
					res[i].Error = err
				} else {
					res[i].Topic.Config = cfg
				}
			}
			if len(req.Metrics) > 0 {
				for _, metric := range metrics[topic.Name] {
					value := int(metric.Value)
					switch metric.Type {
					case "Log":
						i, err := strconv.Atoi(metric.Partition)
						if err != nil {
							res[i].Error = err
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
