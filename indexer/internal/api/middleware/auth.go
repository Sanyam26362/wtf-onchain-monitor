package middleware

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"worldtradefuture/indexer/internal/api/responses"
)

// RequireOperatorAuth validates operator credentials for protected management routes.
func RequireOperatorAuth(expectedKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if expectedKey == "" {
				// If no operator key is set in configuration, deny access for safety
				responses.WriteError(w, http.StatusUnauthorized, responses.ErrCodeUnauthorized, "operator API key not configured on server")
				return
			}

			// Check Authorization: Bearer <token>
			var providedKey string
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				providedKey = strings.TrimPrefix(authHeader, "Bearer ")
			}

			// Fallback to X-API-Key header
			if providedKey == "" {
				providedKey = r.Header.Get("X-API-Key")
			}

			providedKey = strings.TrimSpace(providedKey)
			if providedKey == "" || subtle.ConstantTimeCompare([]byte(providedKey), []byte(expectedKey)) != 1 {
				responses.WriteError(w, http.StatusUnauthorized, responses.ErrCodeUnauthorized, "unauthorized: invalid or missing operator API key")
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
