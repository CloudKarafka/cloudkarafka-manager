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
	humanize "github.com/dustin/go-humanize"
)

type topics map[string]topic

type topicSlice []topic

func (me topicSlice) Get(i int) interface{} {
	return me[i]
}

func (me topicSlice) TotalCount() int {
	return len(me)
}

type Partition struct {
	Number          int            `json:"number"`
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

type partitions []Partition

func (me partitions) Get(i int) interface{} {
	return me[i]
}

func (me partitions) TotalCount() int {
	return len(me)
}

type topic struct {
	Name       string           `json:"name"`
	Partitions partitions       `json:"partitions"`
	Config     TopicConfig      `json:"config"`
	Deleted    bool             `json:"deleted"`
	Metrics    map[string]int   `json:"metrics"`
	BytesIn    *SimpleTimeSerie `json:"bytes_in"`
	BytesOut   *SimpleTimeSerie `json:"bytes_out"`
}

func (t topic) Size() int {
	sum := 0
	for _, p := range t.Partitions {
		sum += p.Metrics["Size"]
	}
	return sum
}
func (t topic) Messages() int {
	sum := 0
	for _, p := range t.Partitions {
		sum += p.Metrics["LogEndOffset"] - p.Metrics["LogStartOffset"]
	}
	return sum
}

func (t topic) MarshalJSON() ([]byte, error) {
	res := map[string]interface{}{
		"name":       t.Name,
		"deleted":    t.Deleted,
		"partitions": len(t.Partitions),
	}
	if t.BytesIn != nil {
		res["bytes_in"] = t.BytesIn.Points
	}
	if t.BytesOut != nil {
		res["bytes_out"] = t.BytesOut.Points
	}
	if len(t.Metrics) > 0 {
		res["metrics"] = t.Metrics
	}
	if len(t.Config.Data) > 0 {
		res["config"] = t.Config
	}
	if v := t.Size(); v != 0 {
		res["size"] = humanize.Bytes(uint64(v))
	}
	if v := t.Messages(); v != 0 {
		res["message_count"] = v
	}
	return json.Marshal(res)
}

type TopicResponse struct {
	Topic topic
	Error error
}

type TopicRequest struct {
	TopicNames []string
	Config     bool
	Metrics    []MetricRequest
}

func fetchTopic(topicName string) (topic, error) {
	tp, err := zookeeper.Topic(topicName)
	if err != nil {
		if err == zookeeper.PathDoesNotExistsErr {
			fmt.Fprintf(os.Stderr, "[INFO] FetchTopic: topic %s does not exists in zookeeper", topicName)
		} else {
			fmt.Fprintf(os.Stderr, "[INFO] FetchTopic: %s", err)
		}
		return topic{}, err
	}
	t := topic{
		Name:       topicName,
		Partitions: make([]Partition, len(tp.Partitions)),
		Metrics:    make(map[string]int),
		BytesIn:    NewSimpleTimeSerie(5, MaxPoints),
		BytesOut:   NewSimpleTimeSerie(5, MaxPoints),
	}
	for p, replicas := range tp.Partitions {
		var par Partition
		partitionPath := fmt.Sprintf("/brokers/topics/%s/partitions/%s/state", topicName, p)
		if err := zookeeper.Get(partitionPath, &par); err == nil {
			i, _ := strconv.Atoi(p)
			par.Replicas = replicas
			par.Metrics = make(map[string]int)
			par.Number = i
			t.Partitions[i] = par
		}
	}
	return t, nil
}

func fetchConfig(ctx context.Context, topicName string) (TopicConfig, error) {
	var topicConfig TopicConfig
	err := zookeeper.Get(fmt.Sprintf("/config/topics/%s", topicName), &topicConfig)
	return topicConfig, err
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
