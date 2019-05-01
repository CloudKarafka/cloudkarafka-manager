package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/cloudkarafka/cloudkarafka-manager/log"
)

var (
	TimeRequests       = false
	RequestTimedOutErr = errors.New("Request timed out")
)

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
	defer r.Body.Close()
	if err != nil {
		return nil, err
	}
	if r.StatusCode != 200 {
		log.Error("doRequest", log.MapEntry{"url": url, "status": r.StatusCode})
		return nil, fmt.Errorf("URL %s returned %d", url, r.StatusCode)
	}
	if err = json.NewDecoder(r.Body).Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func QueryBroker(brokerId int, bean, attr, group string) ([]Metric, error) {
	host := config.BrokerUrls.HttpUrl(brokerId)
	if host == "" {
		return nil, fmt.Errorf("Broker %d not available", brokerId)
	}
	url := fmt.Sprintf("%s/jmx?bean=%s&attrs=%s", host, bean, attr)
	v, err := doRequest(url)
	if err == nil {
		for i, _ := range v {
			v[i].Broker = brokerId
		}
	}
	return v, err
}

func QueryBrokerAsync(brokerId int, query, attribute string, ch chan<- []Metric) {
	data, err := QueryBroker(brokerId, query, attribute, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "[WARN] Could not fetch bean %s from broker %d: %s\n", query, brokerId, err)
		ch <- []Metric{}
	} else {
		ch <- data
	}
}

func getSimpleValue(url string) (string, error) {
	start := time.Now()
	r, err := http.Get(url)
	if err != nil {
		return "", err
	}
	if r.StatusCode != 200 {
		return "", nil
	}
	if TimeRequests {
		fmt.Fprintf(os.Stderr, "Request GET %s took %.2fs\n", url, time.Since(start).Seconds())
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
