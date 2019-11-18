package processes

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/cloudkarafka/cloudkarafka-manager/config"
)

func StopKafka() error {
	fmt.Fprintf(os.Stderr, "[INFO] Stopping kafka\n")
	cmd := exec.Command(filepath.Join(config.KafkaDir, "bin/kafka-server-stop.sh"))
	fmt.Println(cmd.Args)
	out, err := cmd.Output()
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Stop kafka cmd failed: %s\n%s", err, out)
		return fmt.Errorf("Failed to stop kafka")
	}
	fmt.Fprintf(os.Stderr, "[INFO] Kafka stopped\n")
	return nil
}
