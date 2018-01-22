package jmx

import (
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

	ReadTimeout  = errors.New("JmxTerm didn't send any response in time")
	NoSuchBean   = errors.New("No such Bean")
	NoJmxMetrics = errors.New("JMX metrics is not enabled")
	NoKafka      = errors.New("No Kakfa process is running")
	JmxNotLoaded = errors.New("Kafka is starting up but not ready")

	reconnecting = false
)

func Start() {
	fmt.Println("[INFO] JMX connecting")
	if err := connect(); err != nil {
		go reconnect()
		fmt.Println("[ERROR] JMX failed to start", err)
	} else {
		KafkaVersion, _ = run(fmt.Sprintf("get -s -b kafka.server:type=app-info,id=%s Version", BrokerId), 5*time.Second)
		fmt.Printf("[INFO] JMX connected BrokerId=%s KafkaVersion=%s\n", BrokerId, KafkaVersion)
	}
}

func BrokerMetrics(id string) (BrokerMetric, error) {
	bm := BrokerMetric{
		KafkaVersion: KafkaVersion,
		BrokerId:     BrokerId,
	}
	bi, err := BrokerTopicMetric("BytesInPerSec", "")
	if err != nil {
		return bm, err
	}
	bo, err := BrokerTopicMetric("BytesOutPerSec", "")
	if err != nil {
		return bm, err
	}
	mi, err := BrokerTopicMetric("MessagesInPerSec", "")
	if err != nil {
		return bm, err
	}
	bm = BrokerMetric{
		TransferMetric: TransferMetric{
			BytesInPerSec:    bi,
			BytesOutPerSec:   bo,
			MessagesInPerSec: mi,
		},
		KafkaVersion: KafkaVersion,
		BrokerId:     BrokerId,
	}
	return bm, nil
}

// If t is empty string BrokerTopicMetric for entire cluster is returned
func BrokerTopicMetric(name, t string) (float64, error) {
	c := fmt.Sprintf("get -s -b kafka.server:type=BrokerTopicMetrics,name=%s", name)
	if t != "" {
		c = fmt.Sprintf("%s,topic=%s", c, t)
	}
	c = c + " OneMinuteRate"
	raw, err := run(c, 2*time.Second)
	if err != nil {
		return 0, err
	}
	in, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, err
	}
	f, err := strconv.ParseFloat(fmt.Sprintf("%.2f", in), 64)
	if err != nil {
		return 0, err
	}
	return f, nil
}

// Takes name, topic and partition as arguments
func LogOffset(n, t, p string) (int, error) {
	c := fmt.Sprintf("get -s -b kafka.log:type=Log,name=%s,topic=%s,partition=%s Value", n, t, p)
	raw, err := run(c, 2*time.Second)
	if err != nil {
		return 0, err
	}
	o, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return o, nil
}

func Exit() {
	if cmd == nil {
		return
	}
	disconnect()
}

func reconnect() {
	fmt.Println("reconnect")
	if reconnecting {
		return
	}
	reconnecting = true
	for {
		err := connect()
		if err == nil {
			break
		}
		time.Sleep(10 * time.Second)
	}
	reconnecting = false
}

func connect() error {
	l.Lock()
	defer l.Unlock()
	if err := brokerId(); err != nil {
		return err
	}
	var err error
	cmd = exec.Command("java", "-jar", "jars/jmxterm-1.0.0-uber.jar", "-n", "-l", "localhost:9010")
	se, _ := cmd.StderrPipe()
	stderr = bufio.NewScanner(se)
	so, _ := cmd.StdoutPipe()
	stdout = bufio.NewScanner(so)
	stdin, _ = cmd.StdinPipe()
	err = cmd.Start()
	if err != nil {
		return err
	}
	stderr.Scan()
	return nil
}

func disconnect() {
	cmd.Process.Kill()
	cmd.Wait()
	cmd = nil
	stdout = nil
	stderr = nil
	KafkaVersion = *new(string)
	BrokerId = *new(string)
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
	BrokerId = matches[1]
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
		return "", fmt.Errorf("No such bean: %s", str)
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
