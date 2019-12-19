package store

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/zookeeper"
	humanize "github.com/dustin/go-humanize"
)

type brokers map[string]broker

type broker struct {
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
	BytesIn      *SimpleTimeSerie
	BytesOut     *SimpleTimeSerie
}

func (b broker) Online() bool {
	for _, id := range config.BrokerUrls.IDs() {
		if id == b.Id {
			return true
		}
	}
	return false
}
func NewBroker() broker {
	return broker{BytesIn: NewSimpleTimeSerie(5, MaxPoints), BytesOut: NewSimpleTimeSerie(5, MaxPoints)}
}
func (b broker) Uptime() string {
	if ts, err := strconv.ParseInt(b.Timestamp, 10, 64); err == nil {
		return strings.TrimSpace(humanize.RelTime(time.Now(), time.Unix(ts/1000, 0), "", ""))
	}
	return ""
}

func fetchBroker(id int) (broker, error) {
	b := NewBroker()
	path := fmt.Sprintf("/brokers/ids/%d", id)
	err := zookeeper.Get(path, &b)
	if err != nil {
		return b, err
	}
	if controller, err := zookeeper.Controller(); err != nil {
		return b, err
	} else {
		b.Controller = controller.BrokerId == id
	}
	b.Id = id
	b.Metrics = make(map[string]int)
	version, err := KafkaVersion(id)
	if err != nil {
		return b, err
	}
	b.KafkaVersion = version
	return b, nil
}
