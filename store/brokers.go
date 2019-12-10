package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	humanize "github.com/dustin/go-humanize"
)

type brokers map[string]Broker

type Broker struct {
	Version      int            `json:"-"`
	JmxPort      int            `json:"-"`
	Timestamp    string         `json:"timestamp"`
	Endpoints    []string       `json:"endpoints"`
	Host         string         `json:"host"`
	Port         int            `json:"port"`
	Id           int            `json:"id"`
	KafkaVersion string         `json:"kafka_version"`
	Controller   bool           `json:"controller"`
	Metrics      map[string]int `json:"metrics"`
}

func (b Broker) Online() bool {
	for _, id := range config.BrokerUrls.IDs() {
		if id == b.Id {
			return true
		}
	}
	return false
}
func (b Broker) Uptime() string {
	if ts, err := strconv.ParseInt(b.Timestamp, 10, 64); err == nil {
		return strings.TrimSpace(humanize.RelTime(time.Now(), time.Unix(ts/1000, 0), "", ""))
	}
	return ""
}

type BrokerResponse struct {
	Broker Broker
	Error  error
}

func fetchBroker(ctx context.Context, id int) (Broker, error) {
	var broker Broker
	path := fmt.Sprintf("/brokers/ids/%d", id)
	err := zookeeper.Get(path, &broker)
	if err != nil {
		return broker, err
	}
	if controller, err := zookeeper.Controller(); err != nil {
		return broker, err
	} else {
		broker.Controller = controller.BrokerId == id
	}
	broker.Id = id
	broker.Metrics = make(map[string]int)
	version, err := KafkaVersion(id)
	if err != nil {
		return broker, err
	}
	broker.KafkaVersion = version
	store.Brokers[string(id)] = broker
	return broker, nil
}

func fetchBrokerMetrics(ctx context.Context, metrics []MetricRequest) (map[int][]Metric, error) {
	res := make(map[int][]Metric)
	ch := GetMetricsAsync(metrics)
	l := len(metrics)
	for i := 0; i < l; i++ {
		select {
		case response := <-ch:
			if response.Error != nil {
				return res, response.Error
			} else {
				for _, metric := range response.Metrics {
					res[metric.Broker] = append(res[metric.Broker], metric)
				}
			}
		case <-ctx.Done():
			return res, fmt.Errorf("Fetching broker metrics failed: %s", ctx.Err())
		}
	}
	return res, nil
}

func FetchBrokers(ctx context.Context, brokerIds []int, metricReqs []MetricRequest) ([]BrokerResponse, error) {
	var (
		res     = make([]BrokerResponse, len(brokerIds))
		err     error
		metrics map[int][]Metric
	)
	if len(metricReqs) > 0 {
		if metrics, err = fetchBrokerMetrics(ctx, metricReqs); err != nil {
			return nil, err
		}
	}
	for i, id := range brokerIds {
		broker, err := fetchBroker(ctx, id)
		res[i] = BrokerResponse{}
		if err != nil {
			res[i].Error = fmt.Errorf("Failed to fetch info for broker %d from Zookeeper: %s", id, err)
		} else {
			res[i].Broker = broker
			if len(metricReqs) > 0 {
				for _, metric := range metrics[broker.Id] {
					value := int(metric.Value)
					switch metric.Type {
					case "socket-server-metrics":
						n := "Connections" + metric.Listener
						broker.Metrics[n] += value
					default:
						broker.Metrics[metric.Name] += value
					}
				}
			}
		}
	}
	return res, nil
}

func FetchBroker(ctx context.Context, brokerId int, metricReqs []MetricRequest) (Broker, error) {
	res, err := FetchBrokers(ctx, []int{brokerId}, metricReqs)
	if err != nil {
		return Broker{}, err
	}
	if len(res) == 0 {
		return Broker{}, fmt.Errorf("Broker %d not found", brokerId)
	}
	if res[0].Error != nil {
		return Broker{}, res[0].Error
	}
	return res[0].Broker, nil
}
