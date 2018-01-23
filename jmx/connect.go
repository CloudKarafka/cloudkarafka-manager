package jmx

import (
	"bufio"
	"os/exec"
	"time"
)

var (
	reconnecting = false
)

func reconnect() {
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

func disconnect() {
	cmd.Process.Kill()
	cmd.Wait()
	cmd = nil
	stdout = nil
	stderr = nil
	KafkaVersion = *new(string)
	BrokerId = *new(string)
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
	return nil
}
