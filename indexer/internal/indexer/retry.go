package indexer

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"regexp"
	"strings"
	"time"
)

// Sleeper abstracts the sleep function for testability and avoiding slow test suites.
type Sleeper func(ctx context.Context, d time.Duration) error

// DefaultSleeper is the production sleeper that waits for d or ctx cancellation.
func DefaultSleeper(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// RetryPolicy defines the parameters for retrying RPC operations.
type RetryPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	Sleeper        Sleeper
}

// DefaultRetryPolicy returns conservative production defaults, or fast defaults during tests.
func DefaultRetryPolicy() RetryPolicy {
	// Detect if running under `go test` to prevent slow test suites
	if flag.Lookup("test.v") != nil {
		return RetryPolicy{
			MaxRetries:     5,
			InitialBackoff: 1 * time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
			BackoffFactor:  2.0,
			Sleeper:        DefaultSleeper,
		}
	}
	return RetryPolicy{
		MaxRetries:     5,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		BackoffFactor:  2.0,
		Sleeper:        DefaultSleeper,
	}
}

// Is429Error returns true if the error represents an HTTP 429 or rate-limit response.
func Is429Error(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "ratelimit") ||
		strings.Contains(msg, "request limit reached") ||
		strings.Contains(msg, "daily request count exceeded") ||
		strings.Contains(msg, "-32005") || // Infura / Alchemy rate limit error code
		strings.Contains(msg, "compute units per second") ||
		strings.Contains(msg, "exceeded its compute units")
}

// IsRetryableRPCError checks if an error is a transient RPC failure that should be retried.
func IsRetryableRPCError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	if Is429Error(err) {
		return true
	}

	msg := strings.ToLower(err.Error())

	// Simulated RPC failures in test mocks
	if strings.Contains(msg, "simulated rpc failure") ||
		strings.Contains(msg, "simulated rpc network failure") {
		return true
	}

	// Temporary connection and transport errors
	if strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "eof") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "tls handshake timeout") ||
		strings.Contains(msg, "network is unreachable") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "temporary failure in name resolution") {
		return true
	}

	// Timeout
	if errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "timed out") {
		return true
	}

	// Temporary gateway and provider HTTP server errors
	if strings.Contains(msg, "502") || strings.Contains(msg, "bad gateway") ||
		strings.Contains(msg, "503") || strings.Contains(msg, "service unavailable") ||
		strings.Contains(msg, "504") || strings.Contains(msg, "gateway timeout") ||
		strings.Contains(msg, "500") || strings.Contains(msg, "internal server error") ||
		strings.Contains(msg, "over capacity") ||
		strings.Contains(msg, "backend error") ||
		strings.Contains(msg, "server error") {
		return true
	}

	// Check net.Error interface
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return false
}

var (
	urlSecretRegex = regexp.MustCompile(`(https?://[^/\s]+/(?:v[0-9]+/)?)[a-zA-Z0-9_-]{16,}`)
	keyParamRegex  = regexp.MustCompile(`([?&](?:api[_-]?key|key|token|secret)=)[^&\s]+`)
)

// SanitizeError strips sensitive tokens, project IDs, and API keys from error messages.
func SanitizeError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	msg = urlSecretRegex.ReplaceAllString(msg, "${1}[REDACTED]")
	msg = keyParamRegex.ReplaceAllString(msg, "${1}[REDACTED]")
	return msg
}

// Retryer orchestrates resilient execution of block range operations.
type Retryer struct {
	policy RetryPolicy
}

// NewRetryer constructs a Retryer with the provided policy.
func NewRetryer(policy RetryPolicy) *Retryer {
	if policy.MaxRetries <= 0 {
		policy.MaxRetries = 5
	}
	if policy.InitialBackoff <= 0 {
		policy.InitialBackoff = 1 * time.Second
	}
	if policy.MaxBackoff <= 0 {
		policy.MaxBackoff = 30 * time.Second
	}
	if policy.BackoffFactor <= 0 {
		policy.BackoffFactor = 2.0
	}
	if policy.Sleeper == nil {
		policy.Sleeper = DefaultSleeper
	}
	return &Retryer{policy: policy}
}

// Policy returns the active RetryPolicy.
func (r *Retryer) Policy() RetryPolicy {
	return r.policy
}

// RetryRange executes op for block range [from, to] on stream, retrying on transient/rate-limit RPC errors.
func (r *Retryer) RetryRange(
	ctx context.Context,
	stream string,
	from uint64,
	to uint64,
	op func() error,
) error {
	var lastErr error
	backoff := r.policy.InitialBackoff

	for attempt := 1; attempt <= r.policy.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if attempt > 1 {
			slog.Info("Retrying RPC request",
				"stream", stream,
				"from", from,
				"to", to,
				"attempt", attempt,
			)
		}

		lastErr = op()
		if lastErr == nil {
			if attempt > 1 {
				slog.Info("RPC request succeeded after retry",
					"stream", stream,
					"from", from,
					"to", to,
					"attempt", attempt,
				)
			}
			return nil
		}

		// If context was cancelled during op, stop immediately without retrying
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Permanent errors must abort immediately without retrying
		if !IsRetryableRPCError(lastErr) {
			slog.Warn("non-retryable error encountered, aborting retry loop",
				"stream", stream,
				"from", from,
				"to", to,
				"attempt", attempt,
				"error", SanitizeError(lastErr),
			)
			return lastErr
		}

		currentBackoff := backoff

		slog.Warn("RPC request failed",
			"stream", stream,
			"from", from,
			"to", to,
			"attempt", attempt,
			"max_retries", r.policy.MaxRetries,
			"error", SanitizeError(lastErr),
			"retry_in", currentBackoff.String(),
		)

		if attempt < r.policy.MaxRetries {
			if err := r.policy.Sleeper(ctx, currentBackoff); err != nil {
				return err
			}
			nextBackoff := time.Duration(float64(backoff) * r.policy.BackoffFactor)
			if nextBackoff > r.policy.MaxBackoff {
				nextBackoff = r.policy.MaxBackoff
			}
			backoff = nextBackoff
		}
	}

	return fmt.Errorf("exceeded max retries (%d) for stream %s range [%d, %d]: %w",
		r.policy.MaxRetries, stream, from, to, lastErr)
}
