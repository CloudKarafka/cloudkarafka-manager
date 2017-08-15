package jmx

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

var (
	cmd *exec.Cmd
	out bytes.Buffer
	in  io.WriteCloser
	buf = bufio.NewScanner(&out)
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
	pid := "83889"
	start(pid)
}

func KafkaVersion() string {
	return run("get -s -b kafka.server:type=app-info,id=0 Version")
}

func Exit() {
	run("exit")
	fmt.Println(string(out.Bytes()))
	fmt.Println("wait")
	cmd.Wait()
}

func run(s string) string {
	in.Write([]byte(s + "\n"))
	time.Sleep(50 * time.Millisecond)
	txt, err := out.ReadString('\n')
	if err != nil {
		fmt.Println("[INFO]", "JMX term", err)
	}
	return strings.Trim(txt, "\r\n")
}

func start(pid string) {
	cmd.Start()
	run(fmt.Sprintf("open %s", pid))
	time.Sleep(3 * time.Second)
}
