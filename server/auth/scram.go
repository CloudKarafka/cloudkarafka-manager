package auth

import (
	"golang.org/x/crypto/pbkdf2"

	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"fmt"
	"hash"
	"math/rand"
	"time"
)

const (
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func CreateScramLogin(pass, crypt string) (string, string, string, int) {
	enc := base64.StdEncoding.Strict()
	salt := generateSalt()
	var storedKey, serverKey string
	if crypt == "SCRAM-SHA-512" {
		storedKey, serverKey = CalculateSha512Keys(pass, salt)
	} else {
		storedKey, serverKey = CalculateSha256Keys(pass, salt)
	}
	return enc.EncodeToString(salt), storedKey, serverKey, 4096
}

func CalculateSha256Keys(pass string, salt []byte) (string, string) {
	return calculateKeys([]byte(pass), salt, sha256.New)
}

func CalculateSha512Keys(pass string, salt []byte) (string, string) {
	return calculateKeys([]byte(pass), salt, sha512.New)
}

func calculateKeys(pass, salt []byte, hashFn func() hash.Hash) (string, string) {
	enc := base64.StdEncoding.Strict()
	saltedPassword := hi(pass, salt, 4096, hashFn)
	clientKey := calcHmac(hashFn, saltedPassword, []byte("Client Key"))
	fmt.Println(enc.EncodeToString(clientKey))
	storedKey := enc.EncodeToString(h(hashFn, clientKey))
	serverKey := enc.EncodeToString(calcHmac(hashFn, saltedPassword, []byte("Server Key")))
	return storedKey, serverKey
}

func generateSalt() []byte {
	src := rand.NewSource(time.Now().UnixNano())
	n := 32
	b := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return b
}

func hi(password, salt []byte, ittr int, hashFn func() hash.Hash) []byte {
	saltedPassword := pbkdf2.Key(password, salt, ittr, hashFn().Size(), hashFn)
	return saltedPassword
}

func calcHmac(hashFn func() hash.Hash, saltedPassword, msg []byte) []byte {
	hash := hmac.New(hashFn, saltedPassword)
	hash.Write(msg)
	return hash.Sum(nil)
}

func h(hashFn func() hash.Hash, bytes []byte) []byte {
	hash := hashFn()
	hash.Write(bytes)
	return hash.Sum(nil)
}
