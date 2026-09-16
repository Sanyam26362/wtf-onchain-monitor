package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"

	"worldtradefuture/indexer/internal/api/responses"
)

// Recoverer recovers from panics and writes a standardized 500 JSON error envelope.
func Recoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rvr := recover(); rvr != nil {
					stack := debug.Stack()
					reqID := GetRequestID(r.Context())

					logger.Error("panic recovered in HTTP handler",
						slog.Any("panic", rvr),
						slog.String("stack", string(stack)),
						slog.String("request_id", reqID),
						slog.String("path", r.URL.Path),
						slog.String("method", r.Method),
					)

					responses.WriteError(
						w,
						http.StatusInternalServerError,
						responses.ErrCodeInternalError,
						fmt.Sprintf("Internal server error: %v", rvr),
					)
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}
