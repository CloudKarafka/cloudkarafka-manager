package store

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"
)

var (
	jmxCache1Min = cache.New(1*time.Minute, 1*time.Minute)
	plainCache   = cache.New(1*time.Hour, 1*time.Hour)
)

type PluginRequest interface {
	BrokerId() int
	Query() []byte
}

type MetricRequest struct {
	brokerId int
	Bean     JMXBean
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

func (me MetricRequest) BrokerId() int {
	return me.brokerId
}

func (me MetricRequest) Query() []byte {
	return []byte(fmt.Sprintf("jmx %s\n", me.Bean.String()))
}

type GroupRequest struct {
	brokerId int
	group    string
}

func (me GroupRequest) BrokerId() int {
	return me.brokerId
}

func (me GroupRequest) Query() []byte {
	if me.group == "" {
		return []byte("groups")
	}
	return []byte(fmt.Sprintf("grouops %s\n", me.group))
}

type Version struct {
	Broker  int
	Name    string
	Version string
}
type VersionRequest struct {
	brokerId int
	name     string
}

func (me VersionRequest) BrokerId() int {
	return me.brokerId
}

func (me VersionRequest) Query() []byte {
	return []byte(fmt.Sprintf("v %s\n", me.name))
}

type JMXConn struct {
	conn    net.Conn
	scanner *bufio.Scanner
}

type JMXBroker struct {
	conns map[int]JMXConn
}

func (jb *JMXBroker) Connect(brokerId int, uri string) error {
	conn, err := net.Dial("tcp", uri)
	if err != nil {
		return err
	}
	jb.conns[brokerId] = JMXConn{conn, bufio.NewScanner(conn)}
	return err
}

func (jb *JMXBroker) CloseAll() {
	for i, _ := range jb.conns {
		jb.Close(i)
	}
}

func (jb *JMXBroker) Close(brokerId int) {
	if c, ok := jb.conns[brokerId]; ok {
		c.conn.Close()
	}
	delete(jb.conns, brokerId)
}

func (jb *JMXBroker) Write(query PluginRequest) ([]map[string]string, error) {
	var (
		c  JMXConn
		ok bool
	)
	if query.BrokerId() == -1 {
		ok = false
		for _, c1 := range jb.conns {
			c = c1
			ok = true
			break
		}
		if !ok {
			return nil, errors.New("No jmx connections available")
		}
	} else {
		c, ok = jb.conns[query.BrokerId()]
		if !ok {
			return nil, fmt.Errorf("No jmx connection for broker %d", query.BrokerId)
		}
	}
	_, err := c.conn.Write(query.Query())
	if err != nil {
		return nil, err
	}
	var res []map[string]string
	for c.scanner.Scan() {
		line := c.scanner.Text()
		if line == "" {
			break
		}
		cols := strings.Split(line, ";;")
		row := make(map[string]string)
		for _, col := range cols {
			values := strings.Split(col, "=")
			row[values[0]] = values[1]
		}
		res = append(res, row)
	}
	return res, nil
}

func GetMetrics(b *JMXBroker, query MetricRequest, out chan Metric) error {
	res, err := b.Write(query)
	if err != nil {
		return err
	}
	for _, row := range res {
		var m Metric
		m.Broker = query.BrokerId()
		for key, val := range row {
			switch strings.ToLower(key) {
			case "topic":
				m.Topic = val
			case "name":
				m.Name = val
			case "partition":
				m.Partition = val
			case "type":
				m.Type = val
			case "count":
				m.Value, _ = strconv.ParseFloat(val, 32)
			case "value":
				m.Value, _ = strconv.ParseFloat(val, 32)
			case "listener":
				m.Listener = val
			case "networkprocessor":
				m.NetworkProcessor = val
			case "attribute":
				m.Attribute = "Count"
			}
		}
		out <- m
	}
	return nil
}

func GetVersion(b *JMXBroker, query VersionRequest, out chan Version) error {
	var (
		err error
		res []map[string]string
		m   Version
	)
	res, err = b.Write(query)
	if err != nil {
		return err
	}
	m.Broker = query.BrokerId()
	m.Name = query.name
	m.Version = res[0][query.name]
	out <- m
	return nil
}

func GetGroups(b *JMXBroker, out chan ConsumerGroup) error {
	var (
		err    error
		res    []map[string]string
		query  = GroupRequest{-1, ""}
		groups = make(map[string][]ConsumerGroupClient)
	)
	res, err = b.Write(query)
	if err != nil {
		return err
	}
	for _, row := range res {
		name := row["group"]
		partition, _ := strconv.Atoi(row["partition"])
		offset, _ := strconv.Atoi(row["current_offset"])
		m := ConsumerGroupClient{
			Topic:         row["topic"],
			ClientId:      row["clientid"],
			ConsumerId:    row["consumerid"],
			Host:          row["Host"],
			Partition:     partition,
			CurrentOffset: offset,
		}
		groups[name] = append(groups[name], m)

	}
	for name, clients := range groups {
		out <- ConsumerGroup{name, time.Now().Unix(), clients}
	}
	return nil
}
