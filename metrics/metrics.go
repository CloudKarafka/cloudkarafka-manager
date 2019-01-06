package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"time"
)

type BrokerURLs map[int]string

var (
	TimeRequests       = false
	BrokerUrls         BrokerURLs
	RequestTimedOutErr = errors.New("Request timed out")
)

func (b BrokerURLs) Rand() string {
	i := rand.Intn(len(b))
	var k int
	for k = range b {
		if i == 0 {
			break
		}
		i--
	}
	return b[k]
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
}

// Cache request for some seconds?
// Register failed requests, if X fails within interval Y pause requests or stop logging
func QueryBroker(brokerId int, bean, attr, group string) ([]Metric, error) {
	var (
		err error
		v   []Metric
		r   *http.Response
	)
	url := fmt.Sprintf("%s/jmx?bean=%s&attrs=%s", BrokerUrls[brokerId], bean, attr)
	start := time.Now()
	r, err = http.Get(url)
	if TimeRequests {
		fmt.Fprintf(os.Stderr, "Request GET %s took %.4fs\n", url, time.Since(start).Seconds())
	}
	if err != nil {
		return v, err
	}
	if r.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "[INFO] GET %s returned %s\n", url, r.Status)
		return nil, nil
	}
	defer r.Body.Close()
	err = json.NewDecoder(r.Body).Decode(&v)
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
		fmt.Fprintf(os.Stderr, "[INFO] Could not fetch bean %s from %d: %s\n", query, brokerId, err)
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
	url := fmt.Sprintf("%s/kafka-version", BrokerUrls[brokerId])
	return getSimpleValue(url)
}

func PluginVersion(brokerId int) (string, error) {
	url := fmt.Sprintf("%s/plugin-version", BrokerUrls[brokerId])
	return getSimpleValue(url)
}
