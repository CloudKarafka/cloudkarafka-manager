package jmx

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	cmd  *exec.Cmd
	out  bytes.Buffer
	in   io.WriteCloser
	buf  = bufio.NewScanner(&out)
	lock sync.Mutex
)

func Start() {
	cmd = exec.Command("java", "-jar", "jars/jmxterm-1.0.0-uber.jar", "-n", "-v", "silent")
	cmd.Stdout = &out
	cmd.Stderr = &out
	in, _ = cmd.StdinPipe()
	//pid, err := exec.Command("ps", "-C", "java,kafka", "--sort", "cputime", "-o", "pid=", "|tail", "-1").Output()
	//if err != nil {
	//fmt.Println(err)
	//return
	//}
	pid := "5311"
	start(pid)
}

func KafkaVersion(id string) string {
	return run(fmt.Sprintf("get -s -b kafka.server:type=app-info,id=%s Version", id))
}

//If t is empty string BrokerTopicMetric for entire cluster is returned
func BrokerTopicMetric(name, t string) float64 {
	c := fmt.Sprintf("get -s -b kafka.server:type=BrokerTopicMetrics,name=%s", name)
	if t != "" {
		c = fmt.Sprintf("%s,topic=%s", c, t)
	}
	c = c + " OneMinuteRate"
	in, err := strconv.ParseFloat(run(c), 64)
	fmt.Println(in)
	fmt.Println(err)
	f, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", in), 64)
	return f
}

func Exit() {
	run("exit")
	fmt.Println(string(out.Bytes()))
	fmt.Println("wait")
	cmd.Wait()
}

func run(s string) string {
	lock.Lock()
	defer lock.Unlock()
	in.Write([]byte(s + "\n"))
	var (
		txt string
		err error
	)
	for {
		time.Sleep(50 * time.Millisecond)
		txt, err = out.ReadString('\n')
		if txt != "" || err != nil {
			break
		}

	}
	if err != nil && err != io.EOF {
		fmt.Printf("[INFO] package=jmx action=run cmd='%s' err=%s\n", s, err)
	}
	out.Reset()
	return strings.Trim(txt, "\r\n")
}

func start(pid string) {
	cmd.Start()
	run(fmt.Sprintf("open %s", pid))
	time.Sleep(3 * time.Second)
}
