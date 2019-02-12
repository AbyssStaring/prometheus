package logging

import (
	"github.com/go-kit/kit/log"
	"golang.org/x/time/rate"
)

type ratelimiter struct {
	limiter *rate.Limiter
	next    log.Logger
}

// RateLimit write to a loger.
func RateLimit(next log.Logger, limit rate.Limit) log.Logger {
	return &ratelimiter{
		limiter: rate.NewLimiter(limit, int(limit)),
		next:    next,
	}
}

func (r *ratelimiter) Log(keyvals ...interface{}) error {
	if r.limiter.Allow() {
		return r.next.Log(keyvals...)
	}
	return nil
}
