package store

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
	"github.com/patrickmn/go-cache"
)

var jmxCache1Min = cache.New(1*time.Minute, 1*time.Minute)
var plainCache = cache.New(1*time.Hour, 1*time.Hour)

type MetricRequest struct {
	BrokerId int
	Bean     JMXBean
	Attr     string
}

func (mr MetricRequest) String() string {
	return fmt.Sprintf("%d:%s/%s", mr.BrokerId, mr.Bean, mr.Attr)
}

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
	//log.Debug("bean_request", log.MapEntry{"url": url, "status": r.StatusCode})
	if r.StatusCode != 200 {
		log.Warn("bean_request", log.MapEntry{"url": url, "status": r.StatusCode})
		return nil, fmt.Errorf("URL %s returned %d", url, r.StatusCode)
	}
	if err = json.NewDecoder(r.Body).Decode(&v); err != nil {
		return nil, err
	}
	//if len(v) == 0 {
	//	log.Debug("bean_request", log.MapEntry{"body": "[]", "url": url})
	//}
	return v, nil
}

func GetMetrics(query MetricRequest) ([]Metric, error) {
	switch query.Attr {
	case "OneMinuteRate":
		if r, found := jmxCache1Min.Get(query.String()); found {
			log.Info("GetMetrics cached", log.MapEntry{"Bean": query.Bean.String(), "Attr": query.Attr})
			return r.([]Metric), nil
		}
	case "TimeSerie":
		// Never cache
	case "Count":
		// Never cache
	case "Value":
		// Never cache
	default:
		log.Info("GetMetrics nocache", log.MapEntry{"Attr": query.Attr})
	}
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
	switch query.Attr {
	case "OneMinuteRate":
		jmxCache1Min.Set(query.String(), v, cache.DefaultExpiration)
	}
	return v, err
}

func GetMetricsAsync(queries []MetricRequest) <-chan MetricResponse {
	ch := make(chan MetricResponse)
	for _, query := range queries {
		go func(query MetricRequest) {
			data, err := GetMetrics(query)
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
	key := fmt.Sprintf("kafka_version_%d", brokerId)
	if r, found := plainCache.Get(key); found {
		return r.(string), nil
	}
	res, err := getSimpleValue(fmt.Sprintf("%s/kafka-version", config.BrokerUrls.HttpUrl(brokerId)))
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
	res, err := getSimpleValue(fmt.Sprintf("%s/plugin-version", config.BrokerUrls.HttpUrl(brokerId)))
	if err != nil {
		return "", nil
	}
	plainCache.Set(key, res, cache.DefaultExpiration)
	return res, nil

}
