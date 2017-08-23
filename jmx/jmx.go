package jmx

import (
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

var (
	l      sync.Mutex
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser

	ReadTimeout  = errors.New("JmxTerm didn't send any response in time")
	NoSuchBean   = errors.New("No such Bean")
	NoJmxMetrics = errors.New("JMX metrics is not enabled")
)

func Start() {
	cmd = exec.Command("java", "-jar", "jars/jmxterm-1.0.0-uber.jar", "-n")
	stderr, _ = cmd.StderrPipe()
	stdout, _ = cmd.StdoutPipe()
	stdin, _ = cmd.StdinPipe()
	//pid, err := exec.Command("ps", "-C", "java,kafka", "--sort", "cputime", "-o", "pid=", "|tail", "-1").Output()
	//if err != nil {
	//fmt.Println(err)
	//return
	//}
	pid := "52798"
	start(pid)
}

func KafkaVersion(id string) (string, error) {
	return run(fmt.Sprintf("get -s -b kafka.server:type=app-info,id=%s Version", id))
}

//If t is empty string BrokerTopicMetric for entire cluster is returned
func BrokerTopicMetric(name, t string) (float64, error) {
	c := fmt.Sprintf("get -s -b kafka.server:type=BrokerTopicMetrics,name=%s", name)
	if t != "" {
		c = fmt.Sprintf("%s,topic=%s", c, t)
	}
	c = c + " OneMinuteRate"
	raw, err := run(c)
	if err == ReadTimeout && t != "" {
		return 0, nil
	} else if err != nil {
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
	return out, err
}

func start(pid string) {
	cmd.Start()
	read(stderr)
	out, err := run(fmt.Sprintf("open %s", pid))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(out)
}
