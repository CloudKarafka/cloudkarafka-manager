package jmx

import (
	"fmt"
	"io"
	"os/exec"
	"strconv"
	//"strings"
	"sync"
	"time"
)

var (
	cmd    *exec.Cmd
	in     io.WriteCloser
	stdout io.ReadCloser
	lock   = new(sync.Mutex)
)

func Start() {
	cmd = exec.Command("java", "-jar", "jars/jmxterm-1.0.0-uber.jar", "-n", "-v", "silent")
	stdout, _ = cmd.StdoutPipe()
	in, _ = cmd.StdinPipe()
	//pid, err := exec.Command("ps", "-C", "java,kafka", "--sort", "cputime", "-o", "pid=", "|tail", "-1").Output()
	//if err != nil {
	//fmt.Println(err)
	//return
	//}
	pid := "26228"
	start(pid)
}

func KafkaVersion(id string) string {
	return run(fmt.Sprintf("get -s -b kafka.server:type=app-info,id=%s Version", id), false)
}

//If t is empty string BrokerTopicMetric for entire cluster is returned
func BrokerTopicMetric(name, t string) float64 {
	c := fmt.Sprintf("get -s -b kafka.server:type=BrokerTopicMetrics,name=%s", name)
	if t != "" {
		c = fmt.Sprintf("%s,topic=%s", c, t)
	}
	c = c + " OneMinuteRate"
	in, _ := strconv.ParseFloat(run(c, false), 64)
	f, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", in), 64)
	return f
}

// Takes name, topic and partition as arguments
func LogOffset(n, t, p string) int {
	c := fmt.Sprintf("get -s -b kafka.log:type=Log,name=%s,topic=%s,partition=%s Value", n, t, p)
	o, err := strconv.Atoi(run(c, false))
	if err != nil {
		fmt.Println(err)
	}
	return o
}

func Exit() {
	run("exit", true)
	cmd.Wait()
}

func run(s string, skipRead bool) string {
	lock.Lock()
	defer lock.Unlock()
	in.Write([]byte(s + "\n"))
	if skipRead {
		return ""
	}
	buf := make([]byte, 128)
	m := 0
	for {
		b := make([]byte, 16)
		n, err := stdout.Read(b)
		if b[n-1] == '\n' && n == 1 {
			break
		} else if err == io.EOF && n == 0 {
			break
		} else if n == 0 {
			fmt.Println("[ERROR]", err)
			break
		} else if b[n-1] == '\n' {
			m += n - 1
			buf = append(buf[:], b[:n-1]...)
			break
		} else {
			m += n
			buf = append(buf[:], b[:n]...)
		}
	}
	return string(buf[len(buf)-m:])
}

func start(pid string) {
	cmd.Start()
	run(fmt.Sprintf("open %s", pid), true)
	time.Sleep(3 * time.Second)
}
