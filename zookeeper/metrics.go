package zookeeper

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func parseStatValue(key, value string) (interface{}, error) {
	switch key {
	case "received":
		fallthrough
	case "sent":
		fallthrough
	case "connections":
		fallthrough
	case "outstanding":
		fallthrough
	case "node_count":
		return strconv.Atoi(value)
	case "latency_min/avg/max":
		parts := strings.Split(value, "/")
		res := make([]int, len(parts))
		for i, v := range parts {
			if vi, err := strconv.Atoi(v); err == nil {
				res[i] = vi
			}
		}
		return res, nil
	case "clients":
		return []string{}, nil
	default:
		return value, nil
	}
}

func Stats(uri string) map[string]interface{} {
	res := make(map[string]interface{})
	conn, err := net.DialTimeout("tcp", uri, 1000*time.Second)
	defer conn.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not connect to Zk: %s\n", err)
		return res
	}

	fmt.Fprintf(conn, "stat")
	scanner := bufio.NewScanner(conn)
	re := regexp.MustCompile("^[a-zA-Z \\/]+:")
	currentKey := ""
	for scanner.Scan() {
		line := scanner.Text()
		key := re.FindString(line)
		if key != "" {
			currentKey = strings.ToLower(strings.Replace(key, " ", "_", 1))
			currentKey = currentKey[:len(currentKey)-1]
			value := strings.TrimSpace(strings.Split(line, ":")[1])
			v, err := parseStatValue(currentKey, value)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[WARN] parsing zk stats failed: %s\n", err)
			}
			res[currentKey] = v
		} else if line != "" {
			res[currentKey] = append(res[currentKey].([]string), line)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
	return res

}

func Metrics(uri string) map[string]int {
	res := make(map[string]int)
	conn, err := net.DialTimeout("tcp", uri, 1000*time.Second)
	defer conn.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Could not connect to Zk: %s\n", err)
		return res
	}
	fmt.Fprintf(conn, "mntr")
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if v, err := strconv.Atoi(parts[1]); err == nil {
			res[parts[0]] = v
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "reading standard input:", err)
	}
	return res
}
