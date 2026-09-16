package validation_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"worldtradefuture/indexer/internal/api/validation"
)

func TestValidateAddress(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid lowercase", "0x1c7d4b196cb0c7b01d743fbc6116a902379c7238", false},
		{"valid checksum", "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C7238", false},
		{"empty string", "", true},
		{"missing 0x prefix", "1c7D4B196Cb0C7B01d743Fbc6116a902379C7238", true},
		{"invalid length short", "0x1c7D4B196C", true},
		{"invalid length long", "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C72381234", true},
		{"invalid hex characters", "0x1c7D4B196Cb0C7B01d743Fbc6116a902379C72ZZ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validation.ValidateAddress(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateAddress(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestValidateTxHash(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"valid hash", "0x5c4217158bcbe61fe60f852f8623ad9beee4ce2cd1032df4fb3c9c991316b1ff", false},
		{"empty string", "", true},
		{"missing 0x", "5c4217158bcbe61fe60f852f8623ad9beee4ce2cd1032df4fb3c9c991316b1ff", true},
		{"too short", "0x5c4217158bcbe61fe60f852f8623ad9bee", true},
		{"invalid characters", "0x5c4217158bcbe61fe60f852f8623ad9beee4ce2cd1032df4fb3c9c991316b1zz", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validation.ValidateTxHash(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateTxHash(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestParsePagination(t *testing.T) {
	t.Run("default pagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		pag, err := validation.ParsePagination(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pag.Page != 1 || pag.PageSize != validation.DefaultPageSize || pag.Offset != 0 {
			t.Fatalf("unexpected pagination: %+v", pag)
		}
	})

	t.Run("custom valid pagination", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?page=3&page_size=50", nil)
		pag, err := validation.ParsePagination(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pag.Page != 3 || pag.PageSize != 50 || pag.Offset != 100 {
			t.Fatalf("unexpected pagination: %+v", pag)
		}
	})

	t.Run("page less than 1", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?page=0", nil)
		_, err := validation.ParsePagination(req)
		if err == nil {
			t.Fatal("expected error for page=0")
		}
	})

	t.Run("page_size exceeds maximum", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?page_size=200", nil)
		_, err := validation.ParsePagination(req)
		if err == nil {
			t.Fatal("expected error for page_size=200")
		}
	})
}

func TestParseBlockRange(t *testing.T) {
	t.Run("valid range", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?from_block=100&to_block=200", nil)
		bRange, err := validation.ParseBlockRange(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if bRange.FromBlock == nil || *bRange.FromBlock != 100 {
			t.Fatalf("expected FromBlock=100, got %v", bRange.FromBlock)
		}
		if bRange.ToBlock == nil || *bRange.ToBlock != 200 {
			t.Fatalf("expected ToBlock=200, got %v", bRange.ToBlock)
		}
	})

	t.Run("inverted range", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?from_block=300&to_block=200", nil)
		_, err := validation.ParseBlockRange(req)
		if err == nil {
			t.Fatal("expected error for from_block > to_block")
		}
	})

	t.Run("negative block number", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?from_block=-5", nil)
		_, err := validation.ParseBlockRange(req)
		if err == nil {
			t.Fatal("expected error for negative block number")
		}
	})
}

func TestParseDateRange(t *testing.T) {
	t.Run("valid RFC3339 range", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?from_date=2026-09-01T00:00:00Z&to_date=2026-09-10T00:00:00Z", nil)
		dRange, err := validation.ParseDateRange(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if dRange.FromDate == nil || dRange.ToDate == nil {
			t.Fatal("expected non-nil dates")
		}
	})

	t.Run("from date after to date", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?from_date=2026-09-15T00:00:00Z&to_date=2026-09-10T00:00:00Z", nil)
		_, err := validation.ParseDateRange(req)
		if err == nil {
			t.Fatal("expected error when from_date is after to_date")
		}
	})

	t.Run("invalid date format", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?from_date=not-a-date", nil)
		_, err := validation.ParseDateRange(req)
		if err == nil {
			t.Fatal("expected error for invalid date format")
		}
	})
}

func TestParseSort(t *testing.T) {
	allowed := map[string]string{
		"block_number": "block_number",
		"amount":       "amount",
	}

	t.Run("whitelisted sort column", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?sort=amount&order=asc", nil)
		s, err := validation.ParseSort(req, allowed, "block_number", "DESC")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if s.SortBy != "amount" || s.Direction != "ASC" {
			t.Fatalf("unexpected sort: %+v", s)
		}
	})

	t.Run("unauthorized sort column", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?sort=secret_column", nil)
		_, err := validation.ParseSort(req, allowed, "block_number", "DESC")
		if err == nil {
			t.Fatal("expected error for unauthorized sort column")
		}
	})

	t.Run("invalid sort direction", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test?sort=amount&order=sideways", nil)
		_, err := validation.ParseSort(req, allowed, "block_number", "DESC")
		if err == nil {
			t.Fatal("expected error for invalid sort direction")
		}
	})
}
