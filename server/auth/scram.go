package auth

import (
	"golang.org/x/crypto/pbkdf2"

	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"math/rand"
	"time"
)

const (
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func CreateScramLogin(user, pass string) (string, string, string, int) {
	enc := base64.StdEncoding.Strict()
	salt := generateSalt()
	clientKey := enc.EncodeToString(CalculateKey([]byte(pass), []byte("Client Key"), salt, 4096))
	serverKey := enc.EncodeToString(CalculateKey([]byte(pass), []byte("Server Key"), salt, 4096))
	return enc.EncodeToString(salt), clientKey, serverKey, 4096
}

func CalculateKey(password, t, salt []byte, ittr int) []byte {
	key := pbkdf2.Key(password, salt, ittr, sha512.Size, sha512.New)
	hash := hmac.New(sha512.New, key)
	hash.Write([]byte("Client Key"))
	val := sha512.Sum512(hash.Sum(nil))
	return val[:]
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
