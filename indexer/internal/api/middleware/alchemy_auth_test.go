package middleware_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"worldtradefuture/indexer/internal/api/middleware"
)

func TestVerifyAlchemySignature(t *testing.T) {
	key := "test_signing_key_12345"
	body := []byte(`{"webhookId":"wh_123","event":{"data":{"block":{}}}}`)

	mac := hmac.New(sha256.New, []byte(key))
	mac.Write(body)
	validSig := hex.EncodeToString(mac.Sum(nil))

	t.Run("valid signature", func(t *testing.T) {
		if !middleware.VerifyAlchemySignature(body, validSig, key) {
			t.Fatal("expected signature verification to pass")
		}
	})

	t.Run("valid signature with 0x prefix", func(t *testing.T) {
		if !middleware.VerifyAlchemySignature(body, "0x"+validSig, key) {
			t.Fatal("expected signature with 0x prefix to pass")
		}
	})

	t.Run("invalid signature wrong content", func(t *testing.T) {
		invalidSig := validSig[:len(validSig)-2] + "ff"
		if middleware.VerifyAlchemySignature(body, invalidSig, key) {
			t.Fatal("expected signature verification to fail")
		}
	})

	t.Run("tampered body", func(t *testing.T) {
		tamperedBody := []byte(`{"webhookId":"wh_123","event":{"data":{"block":{"tampered":true}}}}`)
		if middleware.VerifyAlchemySignature(tamperedBody, validSig, key) {
			t.Fatal("expected verification to fail for tampered body")
		}
	})

	t.Run("empty signature or key", func(t *testing.T) {
		if middleware.VerifyAlchemySignature(body, "", key) {
			t.Fatal("expected empty signature to fail")
		}
		if middleware.VerifyAlchemySignature(body, validSig, "") {
			t.Fatal("expected empty key to fail")
		}
	})
}
