package web

import (
	"time"

	"github.com/patrickmn/go-cache"
)

var DefaultExpiration = cache.DefaultExpiration
var Cache = cache.New(5*time.Minute, 10*time.Minute)
