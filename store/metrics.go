package store

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/patrickmn/go-cache"
)

var (
	jmxCache1Min = cache.New(1*time.Minute, 1*time.Minute)
	plainCache   = cache.New(1*time.Hour, 1*time.Hour)

	tr = &http.Transport{
		MaxIdleConns:    10,
		IdleConnTimeout: 30 * time.Second,
	}
	httpClient = &http.Client{
		Transport: tr,
	}
)

type MetricRequest struct {
	BrokerId int
	Bean     JMXBean
	Attr     string
}

func (mr MetricRequest) String() string {
	return fmt.Sprintf("%d:%s/%s", mr.BrokerId, mr.Bean, mr.Attr)
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
	Error            string  `json:"-"`
}

func GetMetrics(ctx context.Context, query MetricRequest) ([]Metric, error) {
	switch query.Attr {
	case "OneMinuteRate":
		if r, found := jmxCache1Min.Get(query.String()); found {
			return r.([]Metric), nil
		}
	}
	host := config.BrokerUrls.HttpUrl(query.BrokerId)
	if host == "" {
		return nil, fmt.Errorf("Broker %d not available", query.BrokerId)
	}
	url := fmt.Sprintf("%s/jmx?bean=%s&attrs=%s", host, query.Bean, query.Attr)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	r, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		log.Warn("bean_request", log.MapEntry{"url": req.URL, "status": r.StatusCode})
		return nil, fmt.Errorf("URL %s returned %d", req.URL, r.StatusCode)
	}

	var v []Metric
	if err = json.NewDecoder(r.Body).Decode(&v); err != nil {
		return nil, err
	}

	for i := range v {
		v[i].Broker = query.BrokerId
	}
	switch query.Attr {
	case "OneMinuteRate":
		jmxCache1Min.Set(query.String(), v, cache.DefaultExpiration)
	}
	return v, nil
}

func getSimpleValue(req *http.Request) (string, error) {
	r, err := httpClient.Do(req)
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
	key := fmt.Sprintf("kafka_version_%d", brokerId)
	if r, found := plainCache.Get(key); found {
		return r.(string), nil
	}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/kafka-version", config.BrokerUrls.HttpUrl(brokerId)), nil)
	res, err := getSimpleValue(req)
	if err != nil {
		return "", nil
	}
	plainCache.Set(key, res, cache.DefaultExpiration)
	return res, nil
}

func PluginVersion(brokerId int) (string, error) {
	key := fmt.Sprintf("plugin_version_%d", brokerId)
	if r, found := plainCache.Get(key); found {
		return r.(string), nil
	}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/plugin-version", config.BrokerUrls.HttpUrl(brokerId)), nil)
	res, err := getSimpleValue(req)
	if err != nil {
		return "", nil
	}
	plainCache.Set(key, res, cache.DefaultExpiration)
	return res, nil

}
