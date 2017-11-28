package jmx

import (
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
	stdout       io.ReadCloser
	stderr       io.ReadCloser
	BrokerId     string
	KafkaVersion string

	ReadTimeout  = errors.New("JmxTerm didn't send any response in time")
	NoSuchBean   = errors.New("No such Bean")
	NoJmxMetrics = errors.New("JMX metrics is not enabled")
)

func Start() {
	if err := brokerId(); err != nil {
		fmt.Println("[ERROR]", err)
		return
	}
	cmd = exec.Command("java", "-jar", "jars/jmxterm-1.0.0-uber.jar", "-n", "-l", "localhost:9010")
	stderr, _ = cmd.StderrPipe()
	stdout, _ = cmd.StdoutPipe()
	stdin, _ = cmd.StdinPipe()
	start()
}

type TransferMetric struct {
	BytesInPerSec    float64 `json:"bytes_in_per_sec"`
	BytesOutPerSec   float64 `json:"bytes_out_per_sec"`
	MessagesInPerSec float64 `json:"messages_in_per_sec"`
}

type TopicMetric struct {
	TransferMetric
	MessageCount int `json:"message_count"`
}

type BrokerMetric struct {
	TransferMetric
	KafkaVersion string `json:"kafka_version"`
	BrokerId     string `json:"broker_id"`
}

type OffsetMetric struct {
	LogEndOffset   int `json:"log_end_offset"`
	LogStartOffset int `json:"log_start_offset"`
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
	raw, err := run(c)
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
	raw, err := run(c)
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
	run("exit")
	cmd.Wait()
}

func run(str string) (string, error) {
	if cmd == nil {
		return "", NoJmxMetrics
	}
	l.Lock()
	defer l.Unlock()
	if n, err := stdin.Write([]byte(fmt.Sprintf("%s\n", str))); err != nil || n == 0 {
		return "", err
	}
	head, err := read(stderr)
	if strings.HasPrefix(head, "#IllegalArgumentException") {
		return "", fmt.Errorf("No such bean: %s", str)
	} else if strings.HasPrefix(head, "#Connection") {
		return "", nil
	} else if err != nil && err != io.EOF {
		return "", err
	}
	return read(stdout)
}

func read(reader io.ReadCloser) (string, error) {
	timeout := make(chan error, 1)
	result := make(chan string, 1)
	go func() {
		var (
			m    = 0
			n    = 0
			buff = make([]byte, 128)
			err  error
		)
		for {
			b := make([]byte, 32)
			n, err = reader.Read(b)
			if b[n-1] == '\n' && n == 1 {
				break
			} else if err == io.EOF && n == 0 {
				break
			} else if n == 0 {
				break
			} else if err != nil {
				timeout <- err
			} else if b[n-1] == '\n' {
				m += n - 1
				buff = append(buff[:], b[:n-1]...)
				break
			} else {
				m += n
				buff = append(buff[:], b[:n]...)
			}
		}
		out := string(buff[len(buff)-m:])
		result <- out
	}()
	go func() {
		time.Sleep(2 * time.Second)
		timeout <- ReadTimeout
	}()
	select {
	case res := <-result:
		return res, nil
	case err := <-timeout:
		return "", err
	}
}

func start() {
	err := cmd.Start()
	if err != nil {
		fmt.Println("[ERROR] jmxterm failed to start", err)
	}
	out := make([]byte, 128)
	_, err = stderr.Read(out)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(string(out))
	}
	KafkaVersion, _ = run(fmt.Sprintf("get -s -b kafka.server:type=app-info,id=%s Version", BrokerId))
}

func brokerId() error {
	appInfo := exec.Command("java", "-jar", "jars/jmxterm-1.0.0-uber.jar", "-n", "-l", "localhost:9010")
	in, err := appInfo.StdinPipe()
	if err != nil {
		return err
	}
	go func() {
		defer in.Close()
		io.WriteString(in, "beans -d kafka.server")
	}()
	out, err := appInfo.Output()
	if err != nil {
		return err
	}
	BrokerId = regexp.MustCompile("id=(\\d+)").FindStringSubmatch(string(out))[1]
	return nil
}
