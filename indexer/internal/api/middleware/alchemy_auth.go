package middleware

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"
)

// VerifyAlchemySignature verifies the HMAC-SHA256 signature delivered in the x-alchemy-signature header.
// It computes the HMAC-SHA256 digest of rawBody using signingKey and performs a constant-time comparison.
func VerifyAlchemySignature(rawBody []byte, signatureHex string, signingKey string) bool {
	if signingKey == "" || signatureHex == "" {
		return false
	}

	sig := strings.TrimSpace(signatureHex)
	sig = strings.TrimPrefix(sig, "0x")
	sig = strings.TrimPrefix(sig, "0X")
	sig = strings.ToLower(sig)

	mac := hmac.New(sha256.New, []byte(signingKey))
	mac.Write(rawBody)
	expectedHex := hex.EncodeToString(mac.Sum(nil))

	if len(sig) != len(expectedHex) {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(expectedHex), []byte(sig)) == 1
}
