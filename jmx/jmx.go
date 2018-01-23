package jmx

import (
	"cloudkarafka-mgmt/store"
	"cloudkarafka-mgmt/zookeeper"

	"bufio"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	l            sync.Mutex
	cmd          *exec.Cmd
	stdin        io.WriteCloser
	stdout       *bufio.Scanner
	stderr       *bufio.Scanner
	BrokerId     string
	KafkaVersion string
	loopInterval time.Duration

	ReadTimeout  = errors.New("JmxTerm didn't send any response in time")
	NoSuchBean   = errors.New("No such Bean")
	NoJmxMetrics = errors.New("JMX metrics is not enabled")
	NoKafka      = errors.New("No Kakfa process is running")
	JmxNotLoaded = errors.New("Kafka is starting up but not ready")
)

func Start(interval int) {
	fmt.Println("[INFO] JMX connecting")
	loopInterval = time.Duration(interval) * time.Second
	if err := connect(); err != nil {
		reconnect()
		fmt.Println("[ERROR] JMX failed to start", err)
	} else {
		KafkaVersion, _ = run(fmt.Sprintf("get -s -b kafka.server:type=app-info,id=%s Version", BrokerId), 5*time.Second)
		fmt.Printf("[INFO] JMX connected BrokerId=%s KafkaVersion=%s\n", BrokerId, KafkaVersion)
		loop()
	}
}

func Exit() {
	if cmd == nil {
		return
	}
	disconnect()
}

// If t is empty string BrokerTopicMetrics for entire broker is returned
func BrokerTopicMetrics(name, t string) (int, error) {
	c := fmt.Sprintf("get -s -b kafka.server:type=BrokerTopicMetrics,name=%s", name)
	if t != "" {
		c = fmt.Sprintf("%s,topic=%s", c, t)
	}
	c = c + " OneMinuteRate"
	raw, err := run(c, 2*time.Second)
	if err != nil || raw == "" {
		return 0, err
	}
	rate, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, err
	}
	return int(rate), nil
}

// Takes name, topic and partition as arguments
func LogOffset(n, t, p string) (int, error) {
	c := fmt.Sprintf("get -s -b kafka.log:type=Log,name=%s,topic=%s,partition=%s Value", n, t, p)
	raw, err := run(c, 2*time.Second)
	if err != nil || raw == "" {
		return 0, err
	}
	o, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return o, nil
}

func jmxForAllTopics() error {
	topics, err := zookeeper.Topics(zookeeper.Permissions{Cluster: zookeeper.RW})
	if err != nil {
		return nil
	}
	var returnErr error
	for _, t := range topics {
		topic, err := zookeeper.Topic(t)
		if err != nil {
			break
		}
		bis, err := BrokerTopicMetrics("BytesInPerSec", t)
		if err != nil {
			returnErr = err
			break
		}
		put(map[string]string{"topic": t, "metric": "bytes_in_per_sec"}, bis, []string{"topic"})
		bos, err := BrokerTopicMetrics("BytesOutPerSec", t)
		if err != nil {
			returnErr = err
			break
		}
		put(map[string]string{"topic": t, "metric": "bytes_out_per_sec"}, bos, []string{"topic"})
		mis, err := BrokerTopicMetrics("MessagesInPerSec", t)
		if err != nil {
			returnErr = err
			break
		}
		put(map[string]string{"topic": t, "metric": "messages_in_per_sec"}, mis, []string{"topic"})
		for p, _ := range topic.Partitions {
			so, err := LogOffset("LogStartOffset", t, p)
			if err != nil {
				returnErr = err
				break
			}
			put(map[string]string{"topic": t, "partition": p, "metric": "log_start_offset"}, so, []string{"topic"})
			eo, err := LogOffset("LogEndOffset", t, p)
			if err != nil {
				returnErr = err
				break
			}
			put(map[string]string{"topic": t, "partition": p, "metric": "log_end_offset"}, eo, []string{"topic"})
		}
	}
	return returnErr
}

func jmxForBroker() error {
	bis, err := BrokerTopicMetrics("BytesInPerSec", "")
	if err != nil {
		return err
	}
	put(map[string]string{"broker": BrokerId, "metric": "bytes_in_per_sec"}, bis, []string{"broker"})
	bos, err := BrokerTopicMetrics("BytesOutPerSec", "")
	if err != nil {
		return err
	}
	put(map[string]string{"broker": BrokerId, "metric": "bytes_out_per_sec"}, bos, []string{"broker"})
	mis, err := BrokerTopicMetrics("MessagesInPerSec", "")
	if err != nil {
		return err
	}
	put(map[string]string{"broker": BrokerId, "metric": "messages_in_per_sec"}, mis, []string{"broker"})
	return nil
}

func loop() {
	for {
		err := jmxForAllTopics()
		if err != nil {
			fmt.Println(err)
			disconnect()
			reconnect()
			break
		}
		err = jmxForBroker()
		if err != nil {
			fmt.Println(err)
			disconnect()
			reconnect()
			break
		}
		time.Sleep(loopInterval)
	}
}

func brokerId() error {
	appInfo := exec.Command("java", "-jar", "jars/jmxterm-1.0.0-uber.jar", "-n", "-l", "localhost:9010")
	in, err := appInfo.StdinPipe()
	if err != nil {
		return err
	}
	go func() {
		defer in.Close()
		io.WriteString(in, fmt.Sprintf("beans -d kafka.server\n"))
	}()
	out, err := appInfo.Output()
	if err != nil {
		return err
	}
	matches := regexp.MustCompile(".*app-info.*").FindStringSubmatch(string(out))
	matches = regexp.MustCompile("id=(\\d+)").FindStringSubmatch(string(matches[0]))
	if len(matches) != 2 {
		return JmxNotLoaded
	}
	return nil
}

func run(str string, timeout time.Duration) (string, error) {
	l.Lock()
	defer l.Unlock()
	if cmd == nil {
		return "", NoJmxMetrics
	}
	if n, err := stdin.Write([]byte(fmt.Sprintf("%s\n", str))); err != nil || n == 0 {
		return "", err
	}
	head, err := read(stderr, timeout)
	if strings.HasPrefix(head, "#IllegalArgumentException") {
		return "", nil
	} else if strings.HasPrefix(head, "#Connection") {
		return "", nil
	} else if err != nil && err != io.EOF {
		return "", err
	}
	return read(stdout, timeout)
}

func read(reader *bufio.Scanner, timeout time.Duration) (string, error) {
	result := make(chan string, 1)
	go func() {
		if reader.Scan() {
			result <- strings.TrimSpace(reader.Text())
		} else {
			fmt.Println("[ERROR]", reader.Err())
		}
	}()
	select {
	case res := <-result:
		return res, nil
	case <-time.After(timeout):
		fmt.Println("timeout")
		disconnect()
		go reconnect()
		return "", ReadTimeout
	}
}

func put(key map[string]string, value int, indexOn []string) {
	store.Put(store.Data{Id: key, Value: value}, indexOn)
}
