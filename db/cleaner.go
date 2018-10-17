package db

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

var paths []string = []string{
	"brokers/*/messages_in",
	"brokers/*/bytes_in",
	"brokers/*/bytes_out",
	"brokers/*/bytes_rejected",
	"topics/*/messages_in",
	"topics/*/bytes_in",
	"topics/*/bytes_out",
	"topics/*/bytes_rejected",
}

func clean(upto time.Time, base *bolt.Bucket, parts []string) error {
	if len(parts) == 0 {
		c := base.Cursor()
		max := []byte(upto.Format(time.RFC3339))
		for k, _ := c.First(); k != nil && bytes.Compare(k, max) <= 0; k, _ = c.Next() {
			if err := base.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}
	c := base.Cursor()
	if parts[0] == "*" {
		for k, v := c.First(); k != nil; k, v = c.Next() {
			if v == nil {
				if next := base.Bucket(k); next != nil {
					if e := clean(upto, next, parts[1:]); e != nil {
						return e
					}
				}
			}
		}
	} else {
		if next := base.Bucket([]byte(parts[0])); next != nil {
			return clean(upto, next, parts[1:])
		}
	}
	return nil
}

func Cleaner(upto time.Time) {
	err := db.Update(func(tx *bolt.Tx) error {
		for _, path := range paths {
			fmt.Fprintf(os.Stderr, "[INFO] db clean %s upto %s\n", path, upto.Format(time.RFC3339))
			parts := strings.Split(path, "/")
			b := tx.Bucket([]byte(parts[0]))
			if b != nil {
				if e := clean(upto, b, parts[1:]); e != nil {
					return e
				}
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] Cleaner: %s\n", err)
	}

}
