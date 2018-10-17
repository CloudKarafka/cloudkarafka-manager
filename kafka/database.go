package kafka

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"
)

type MetricKeys struct {
	Name       string `json:"name"`
	MetricType string `json:"type"`
	Request    string `json:"request"`
	Topic      string `json:"topic"`
	Listener   string `json:"listener"`
	Partition  string `json:"partition"`
}

func (mk MetricKeys) String() string {
	return fmt.Sprintf("name=%s,type=%s,request=%s,Topic=%s,listener=%s",
		mk.Name, mk.MetricType, mk.Request, mk.Topic, mk.Listener)
}

type MetricMessage struct {
	Domain     string      `json:"domain"`
	Keys       MetricKeys  `json:"keys"`
	Value      interface{} `json:"value"`
	MetricType string      `json:"metric_type"`
	Alias      string      `json:"alias"`
	Broker     string      `json:"broker"`
	Group      string      `json:"group"`
	Timestamp  time.Time
}

func (mk MetricMessage) String() string {
	return fmt.Sprintf("Domain=%s,Keys=%s,Value=%v", mk.Domain, mk.Keys, mk.Value)
}

func (m MetricMessage) Buckets() []string {
	var base []string
	if m.Group != "" {
		base = []string{"groups",
			m.Group,
			m.Keys.Topic,
			m.Keys.Partition}
	} else if m.Keys.Topic != "" {
		base = []string{"topics", m.Keys.Topic}
		if m.Keys.Partition != "" {
			base = append(base, []string{"partitions", m.Keys.Partition}...)
		}
	} else if m.Broker != "" {
		base = []string{"brokers", m.Broker}
	}
	// Create a new bucket for time series data
	if m.MetricType == "hist" {
		base = append(base, m.Alias)
	}
	return base
}

func (m MetricMessage) key() string {
	if m.MetricType == "hist" {
		return m.Timestamp.Format(time.RFC3339)
	}
	if m.Keys.MetricType == "socket-server-metrics" {
		return ""
	}
	return m.Alias
}

func (m MetricMessage) val() ([]byte, error) {
	switch v := m.Value.(type) {
	case string:

		return []byte(v), nil
	case float64:
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(m.Value)
		if err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}
	return nil, fmt.Errorf("Cannot parse value %v", m.Value)
}

func (m MetricMessage) KeyValue() ([]byte, []byte) {
	k := m.key()
	if v, err := m.val(); err == nil && k != "" {
		return []byte(k), v
	}
	return nil, nil
}
