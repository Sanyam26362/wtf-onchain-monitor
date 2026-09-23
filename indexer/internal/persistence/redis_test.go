package persistence

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestNewRedisClient_Ping(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "localhost:6379"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := NewRedisClient(ctx, redisURL, "", 0)
	if err != nil {
		t.Fatalf("failed to connect to Redis: %v", err)
	}
	defer client.Close()

	pong, err := client.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("failed to ping Redis: %v", err)
	}

	if pong != "PONG" {
		t.Fatalf("expected PONG, got %s", pong)
	}
}
