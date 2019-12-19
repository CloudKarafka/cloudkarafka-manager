package store

import (
	"strconv"
	"strings"
	"sync"
	"time"

	humanize "github.com/dustin/go-humanize"
)

type storage struct {
	sync.RWMutex
	brokers   brokers
	topics    topics
	consumers ConsumerGroups
}

var store = storage{
	brokers:   make(brokers),
	topics:    make(topics),
	consumers: make(ConsumerGroups),
}

func (me storage) UpdateBroker(b broker) {
	me.Lock()
	defer me.Unlock()
	me.brokers[string(b.Id)] = b
}

func (me storage) Brokers() brokers {
	me.RLock()
	defer me.RUnlock()
	return me.brokers
}

func (me storage) Broker(id string) (broker, bool) {
	me.RLock()
	defer me.RUnlock()
	b, ok := me.brokers[id]
	return b, ok
}

func (me storage) Topics() []topic {
	me.RLock()
	defer me.RUnlock()
	var (
		topics = make([]topic, len(me.topics))
		i      = 0
	)
	for _, t := range me.topics {
		topics[i] = t
		i += 1
	}
	return topics
}
func (me storage) Topic(name string) (topic, bool) {
	me.RLock()
	defer me.RUnlock()
	t, ok := me.topics[name]
	return t, ok
}
func (me storage) UpdateTopic(t topic) {
	me.Lock()
	defer me.Unlock()
	me.topics[string(t.Name)] = t
}
func (me *storage) UpdateTopicMetric(m Metric) {
	me.Lock()
	defer me.Unlock()
	t, ok := me.topics[m.Topic]
	if !ok {
		return
	}
	if m.Partition != "" {
		number, err := strconv.Atoi(m.Partition)
		if err != nil {
			return
		}
		p := t.Partitions[number]
		p.Metrics[m.Name] = int(m.Value)
		t.Partitions[number] = p
	} else {
		switch m.Name {
		case "BytesInPerSec":
			t.BytesIn.Add(int(m.Value))
		case "BytesOutPerSec":
			t.BytesOut.Add(int(m.Value))
		}
	}
	me.topics[m.Topic] = t
}

func (me *storage) UpdateBrokerMetrics(m Metric) {
	me.Lock()
	defer me.Unlock()
	b, ok := me.brokers[string(m.Broker)]
	if !ok {
		return
	}
	switch m.Name {
	case "BytesInPerSec":
		b.BytesIn.Add(int(m.Value))
	case "BytesOutPerSec":
		b.BytesOut.Add(int(m.Value))
	}
}
func (me storage) SumBrokerSeries(metric string) TimeSerie {
	me.RLock()
	defer me.RUnlock()
	var (
		series = make([]TimeSerie, len(me.brokers))
		i      = 0
	)
	for _, b := range me.brokers {
		var s TimeSerie
		switch metric {
		case "bytes_in":
			s = b.BytesIn
		case "bytes_out":
			s = b.BytesOut
		}
		series[i] = s
		i += 1
	}
	if len(series) == 0 {
		return &SumTimeSerie{}
	}
	return NewSumTimeSerie(series)
}

func (me storage) Consumers() ConsumerGroups {
	me.RLock()
	defer me.RUnlock()
	return me.consumers
}

func Uptime() string {
	var ts int64
	for _, b := range store.Brokers() {
		tnew, err := strconv.ParseInt(b.Timestamp, 10, 64)
		if err != nil {
			continue
		}
		if ts == 0 || ts < tnew {
			ts = tnew
		}
	}
	if ts == 0 {
		return ""
	}
	return strings.TrimSpace(humanize.RelTime(time.Now(), time.Unix(ts/1000, 0), "", ""))
}

func Brokers() brokers {
	return store.Brokers()
}
func Topics() []topic {
	return store.Topics()
}
func Consumers() ConsumerGroups {
	return store.Consumers()
}
func Partitions() int {
	count := 0
	for _, t := range store.Topics() {
		count += len(t.Partitions)
	}
	return count
}
func TotalTopicSize() int {
	size := 0
	for _, t := range store.Topics() {
		size += t.Size()
	}
	return size
}
func TotalMessageCount() int {
	msgs := 0
	for _, t := range store.Topics() {
		msgs += t.Messages()
	}
	return msgs
}

func Broker(id string) (broker, bool) {
	return store.Broker(id)
}
func Topic(name string) (topic, bool) {
	return store.Topic(name)
}

func SumBrokerSeries(m string) TimeSerie {
	return store.SumBrokerSeries(m)
}
