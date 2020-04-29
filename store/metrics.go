package store

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
	"github.com/patrickmn/go-cache"
)

var (
	jmxCache1Min = cache.New(1*time.Minute, 1*time.Minute)
	plainCache   = cache.New(1*time.Hour, 1*time.Hour)
)

type MetricRequest struct {
	BrokerId int
	Bean     JMXBean
	Attr     string
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

func (me MetricRequest) Bytes() []byte {
	return []byte(fmt.Sprintf("jmx %s\n", me.Bean.String()))
}

type JMXConn struct {
	conn    net.Conn
	scanner *bufio.Scanner
}

func (me JMXConn) Close() {
	me.conn.Close()
}

func DialJMXServer() (JMXConn, error) {
	conn, err := net.Dial("tcp", "localhost:19500")
	if err != nil {
		return JMXConn{}, err
	}
	jmx := JMXConn{conn, bufio.NewScanner(conn)}
	return jmx, nil
}

func (me JMXConn) Write(query MetricRequest) {
	me.conn.Write(query.Bytes())
}

var (
	writes = 0
	reads  = 0
)

func (me JMXConn) GetMetrics(query MetricRequest) ([]Metric, error) {
	_, err := me.conn.Write(query.Bytes())
	var metrics []Metric
	if err != nil {
		return metrics, err
	}
	for me.scanner.Scan() {
		line := me.scanner.Text()
		if line == "" {
			break
		}
		m := Metric{}
		cols := strings.Split(line, ";;")
		for _, col := range cols {
			values := strings.Split(col, "=")
			if len(values) != 2 {
				continue
			}
			key, val := values[0], values[1]
			switch strings.ToLower(key) {
			case "topic":
				m.Topic = val
			case "broker":
				m.Broker, _ = strconv.Atoi(val)
			case "name":
				m.Name = val
			case "partition":
				m.Partition = val
			case "type":
				m.Type = val
			case "count":
				m.Value, _ = strconv.ParseFloat(val, 32)
			case "listener":
				m.Listener = val
			case "networkprocessor":
				m.NetworkProcessor = val
			case "attribute":
				m.Attribute = "Count"
			}
		}
		metrics = append(metrics, m)
	}
	return metrics, nil
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
