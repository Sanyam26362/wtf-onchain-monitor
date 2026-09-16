package validation

import (
	"fmt"
	"net/http"
	"strconv"
)

const (
	DefaultPageSize = 20
	MaxPageSize     = 100
)

// PaginationParams holds validated pagination parameters.
type PaginationParams struct {
	Page     int
	PageSize int
	Offset   int
}

// ParsePagination extracts and validates page and page_size from query parameters.
func ParsePagination(r *http.Request) (PaginationParams, error) {
	page := 1
	pageSize := DefaultPageSize

	q := r.URL.Query()

	if val := q.Get("page"); val != "" {
		p, err := strconv.Atoi(val)
		if err != nil || p < 1 {
			return PaginationParams{}, fmt.Errorf("page must be an integer greater than or equal to 1")
		}
		page = p
	}

	if val := q.Get("page_size"); val != "" {
		ps, err := strconv.Atoi(val)
		if err != nil || ps < 1 {
			return PaginationParams{}, fmt.Errorf("page_size must be an integer greater than or equal to 1")
		}
		if ps > MaxPageSize {
			return PaginationParams{}, fmt.Errorf("page_size exceeds maximum allowed of %d", MaxPageSize)
		}
		pageSize = ps
	}

	offset := (page - 1) * pageSize
	return PaginationParams{
		Page:     page,
		PageSize: pageSize,
		Offset:   offset,
	}, nil
}
