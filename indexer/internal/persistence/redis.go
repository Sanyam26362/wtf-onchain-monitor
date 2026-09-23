package persistence

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// NewRedisClient creates and validates a connection to Redis using the provided address and credentials.
func NewRedisClient(ctx context.Context, addr string, password string, db int) (*redis.Client, error) {
	if addr == "" {
		addr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("failed to connect to Redis at %s: %w", addr, err)
	}

	return rdb, nil
}
