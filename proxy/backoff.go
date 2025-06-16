package main

import (
	"math/rand/v2"
	"time"
)

type backOffConfig struct {
	BaseDelay  time.Duration
	Multiplier float64
	Jitter     float64
	MaxDelay   time.Duration
}

var deffaultBackOffConfig = backOffConfig{
	BaseDelay:  1 * time.Second,
	Multiplier: 1.6,
	Jitter:     0.2,
	MaxDelay:   120 * time.Second,
}

func durationToBackoff(retries int, config backOffConfig) time.Duration {
	if retries == 0 {
		return config.BaseDelay
	}
	backoff, m := float64(config.BaseDelay), float64(config.MaxDelay)
	for backoff < m && retries > 0 {
		backoff *= config.Multiplier
		retries--
	}
	if backoff > m {
		backoff = m
	}
	backoff *= 1 + config.Jitter*(rand.Float64()*2-1)
	if backoff < 0 {
		return 0
	}
	return time.Duration(backoff)
}
