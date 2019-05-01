package store

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
)

type MetricRequest struct {
	BrokerId int
	Bean     string
	Attr     string
}

func BrokerBeanRequest(id int, metric string) MetricRequest {
	bean := fmt.Sprintf("kafka.server:type=BrokerTopicMetrics,name=%s", metric)
	return MetricRequest{id, bean, "Count"}
}

func TopicBeanRequest(id int, topic string, metric string) MetricRequest {
	bean := fmt.Sprintf("kafka.server:type=BrokerTopicMetrics,name=%s,topic=%s", metric, topic)
	return MetricRequest{id, bean, "Count"}
}

var (
	TopicBytesInPerSec = func(id int, topic string) MetricRequest {
		return MetricRequest{id, "kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=" + topic, "OneMinuteRate"}
	}
	TopicBytesOutPerSec = func(id int, topic string) MetricRequest {
		return MetricRequest{id, "kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=" + topic, "OneMinuteRate"}
	}
	PartitionLogStartOffset = func(id int, topic string) MetricRequest {
		return MetricRequest{
			id,
			fmt.Sprintf("kafka.log:type=Log,name=LogStartOffset,topic=%s,partition=*", topic),
			"OneMinuteRate"}
	}
	PartitionLogEndOffset = func(id int, topic string) MetricRequest {
		return MetricRequest{
			id,
			fmt.Sprintf("kafka.log:type=Log,name=LogEndOffset,topic=%s,partition=*", topic),
			"OneMinuteRate"}
	}
	PartitionSize = func(id int, topic string) MetricRequest {
		return MetricRequest{
			id,
			fmt.Sprintf("kafka.log:type=Log,name=Size,topic=%s,partition=*", topic),
			"OneMinuteRate"}
	}
)

type MetricResponse struct {
	Metrics []Metric
	Error   error
}

type Metric struct {
	Broker           int     `json:"broker"`
	Topic            string  `json:"topic"`
	Name             string  `json:"name"`
	Partition        string  `json:"partition"`
	Type             string  `json:"type"`
	Value            float64 `json:"value"`
	Listener         string  `json:"listener"`
	NetworkProcessor string  `json:"networkProcessor"`
	Attribute        string  `json:"attribute"`
	Request          string  `json:"request"`
	Key              string  `json:"key"`
}

func doRequest(url string) ([]Metric, error) {
	var v []Metric
	r, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return nil, fmt.Errorf("URL %s returned %d", url, r.StatusCode)
	}
	if err = json.NewDecoder(r.Body).Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func QueryBroker(query MetricRequest) ([]Metric, error) {
	host := config.BrokerUrls.HttpUrl(query.BrokerId)
	if host == "" {
		return nil, fmt.Errorf("Broker %d not available", query.BrokerId)
	}
	url := fmt.Sprintf("%s/jmx?bean=%s&attrs=%s", host, query.Bean, query.Attr)
	v, err := doRequest(url)
	if err == nil {
		for i, _ := range v {
			v[i].Broker = query.BrokerId
		}
	}
	return v, err
}

func QueryBrokerAsync(queries []MetricRequest) <-chan MetricResponse {
	ch := make(chan MetricResponse)
	for _, query := range queries {
		go func(query MetricRequest) {
			data, err := QueryBroker(query)
			ch <- MetricResponse{data, err}
		}(query)
	}
	return ch
}

func getSimpleValue(url string) (string, error) {
	r, err := http.Get(url)
	if err != nil {
		return "", err
	}
	if r.StatusCode != 200 {
		return "", nil
	}
	body, err := ioutil.ReadAll(r.Body)
	return string(body), err
}

func KafkaVersion(brokerId int) (string, error) {
	url := fmt.Sprintf("%s/kafka-version", config.BrokerUrls.HttpUrl(brokerId))
	return getSimpleValue(url)
}

func PluginVersion(brokerId int) (string, error) {
	url := fmt.Sprintf("%s/plugin-version", config.BrokerUrls.HttpUrl(brokerId))
	return getSimpleValue(url)
}
