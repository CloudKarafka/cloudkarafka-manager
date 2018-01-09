package jmx

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func connect() error {
	pid := kafkaPid()
	if pid == "" {
		return NoKafka
	}
	l.Lock()
	defer l.Unlock()
	if err := brokerId(pid); err != nil {
		return err
	}
	cmd = exec.Command("java", "-jar", "jars/jmxterm-1.0.0-uber.jar", "-n")
	se, _ := cmd.StderrPipe()
	stderr = bufio.NewScanner(se)
	so, _ := cmd.StdoutPipe()
	stdout = bufio.NewScanner(so)
	stdin, _ = cmd.StdinPipe()
	err := cmd.Start()
	if err != nil {
		return err
	}
	stderr.Scan()
	return nil
}

func reconnect() {
	if reconnecting {
		return
	}
	fmt.Println("reconnect")
	reconnecting = true
	for {
		err := connect()
		if err == nil && openPid() == nil {
			break
		}
		time.Sleep(10 * time.Second)
	}
	reconnecting = false
	loop()
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

func brokerId(pid string) error {
	appInfo := exec.Command("java", "-jar", "jars/jmxterm-1.0.0-uber.jar", "-n")
	in, err := appInfo.StdinPipe()
	if err != nil {
		return err
	}
	go func() {
		defer in.Close()
		io.WriteString(in, fmt.Sprintf("open %s\nbeans -d kafka.server", pid))
	}()
	out, err := appInfo.Output()
	if err != nil {
		return err
	}
	matches := regexp.MustCompile("id=(\\d+)").FindStringSubmatch(string(out))
	if len(matches) != 2 {
		return JmxNotLoaded
	}
	BrokerId = matches[1]
	return nil
}

func kafkaPid() string {
	ps, err := exec.Command("pgrep", "-f", "kafka").Output()
	if err != nil {
		fmt.Println(err)
		return ""
	}
	pids := string(ps)
	myPid := strconv.Itoa(os.Getpid())
	pids = strings.Replace(pids, myPid, "", 1)
	return strings.TrimSpace(pids)
}

func openPid() error {
	pid := kafkaPid()
	fmt.Println("open", pid)
	out, err := run(fmt.Sprintf("open %s", pid), 20*time.Second)
	fmt.Println(out, err)
	if err != nil {
		return err
	}
	KafkaVersion, err = run(fmt.Sprintf("get -s -b kafka.server:type=app-info,id=%s Version", BrokerId), 2*time.Second)
	fmt.Println(err)
	return err
}
