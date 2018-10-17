package db

import (
	"fmt"
	"os"
	"time"

	bolt "go.etcd.io/bbolt"
)

type DBValue interface {
	Buckets() []string
	KeyValue() ([]byte, []byte)
}

var db *bolt.DB

func Connect() error {
	var err error
	db, err = bolt.Open("bolt.db", 0600, &bolt.Options{Timeout: 30 * time.Second})
	if err != nil {
		return err
	}
	return nil
}

func View(fn func(*bolt.Tx) error) error {
	err := db.View(fn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] db.View: %s\n", err)
	}
	return err
}

func DeleteBucket(bucketName []byte) {
	err := db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(bucketName)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] db.DeleteBucket: %s %s\n", bucketName, err)
	}
}

type TT interface {
	CreateBucketIfNotExists([]byte) (*bolt.Bucket, error)
}

func Write(msgs []DBValue) error {
	return db.Update(func(tx *bolt.Tx) error {
		for _, msg := range msgs {
			b := TT(tx)
			var err error
			var b2 *bolt.Bucket
			for _, r := range msg.Buckets() {
				if b2, err = b.CreateBucketIfNotExists([]byte(r)); err != nil {
					return err
				}
				b = TT(b2)
			}
			if k, v := msg.KeyValue(); k != nil {
				if err := b2.Put(k, v); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func Close() {
	db.Close()
}
