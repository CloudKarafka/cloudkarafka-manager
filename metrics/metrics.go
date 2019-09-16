package metrics

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
)

var (
	TimeRequests       = false
	RequestTimedOutErr = errors.New("Request timed out")
	transport          = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 15 * time.Second,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     false,
		MaxIdleConns:          10,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	client = &http.Client{Transport: transport}
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

// Cache request for some seconds?
// Register failed requests, if X fails within interval Y pause requests or stop logging
func QueryBroker(brokerId int, bean, attr, group string) ([]Metric, error) {
	var (
		err error
		v   []Metric
		r   *http.Response
	)
	host := config.BrokerUrls.HttpUrl(brokerId)
	if host == "" {
		return v, nil //fmt.Errorf("No URL found to broker %d", brokerId)
	}
	url := fmt.Sprintf("%s/jmx?bean=%s&attrs=%s", config.BrokerUrls.HttpUrl(brokerId), bean, attr)
	start := time.Now()
	r, err = client.Get(url)
	if TimeRequests {
		fmt.Fprintf(os.Stderr, "Request GET %s took %.4fs\n", url, time.Since(start).Seconds())
	}
	if err != nil {
		return v, err
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "[INFO] GET %s returned %s\n", url, r.Status)
		return nil, nil
	}
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
		fmt.Fprintf(os.Stderr, "[WARN] Could not fetch bean %s from broker %d: %s\n", query, brokerId, err)
		ch <- []Metric{}
	} else {
		ch <- data
	}
}

func getSimpleValue(url string) (string, error) {
	start := time.Now()
	r, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer r.Body.Close()
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
