package validation

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// BlockRangeParams holds optional block range boundaries.
type BlockRangeParams struct {
	FromBlock *uint64
	ToBlock   *uint64
}

// ParseBlockRange validates optional from_block and to_block query parameters.
func ParseBlockRange(r *http.Request) (BlockRangeParams, error) {
	q := r.URL.Query()
	var fromBlock, toBlock *uint64

	if val := q.Get("from_block"); val != "" {
		b, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return BlockRangeParams{}, fmt.Errorf("invalid from_block: must be a non-negative integer")
		}
		fromBlock = &b
	}

	if val := q.Get("to_block"); val != "" {
		b, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return BlockRangeParams{}, fmt.Errorf("invalid to_block: must be a non-negative integer")
		}
		toBlock = &b
	}

	if fromBlock != nil && toBlock != nil && *fromBlock > *toBlock {
		return BlockRangeParams{}, fmt.Errorf("from_block (%d) must be less than or equal to to_block (%d)", *fromBlock, *toBlock)
	}

	return BlockRangeParams{
		FromBlock: fromBlock,
		ToBlock:   toBlock,
	}, nil
}

// DateRangeParams holds optional datetime range boundaries.
type DateRangeParams struct {
	FromDate *time.Time
	ToDate   *time.Time
}

// ParseDateRange validates optional from_date and to_date query parameters in RFC3339 or ISO8601 format.
func ParseDateRange(r *http.Request) (DateRangeParams, error) {
	q := r.URL.Query()
	var fromDate, toDate *time.Time

	if val := q.Get("from_date"); val != "" {
		t, err := parseTime(val)
		if err != nil {
			return DateRangeParams{}, fmt.Errorf("invalid from_date format: must be ISO8601/RFC3339 (e.g. 2026-09-16T10:00:00Z)")
		}
		fromDate = &t
	}

	if val := q.Get("to_date"); val != "" {
		t, err := parseTime(val)
		if err != nil {
			return DateRangeParams{}, fmt.Errorf("invalid to_date format: must be ISO8601/RFC3339 (e.g. 2026-09-16T10:00:00Z)")
		}
		toDate = &t
	}

	if fromDate != nil && toDate != nil && fromDate.After(*toDate) {
		return DateRangeParams{}, fmt.Errorf("from_date must be before or equal to to_date")
	}

	return DateRangeParams{
		FromDate: fromDate,
		ToDate:   toDate,
	}, nil
}

func parseTime(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range formats {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized format")
}

// SortParams holds safe, whitelisted sorting options.
type SortParams struct {
	SortBy    string
	Direction string // "ASC" or "DESC"
}

// ParseSort extracts sorting column and direction against a strict whitelist.
func ParseSort(r *http.Request, allowedColumns map[string]string, defaultCol string, defaultDir string) (SortParams, error) {
	q := r.URL.Query()

	sortBy := defaultCol
	if val := strings.ToLower(strings.TrimSpace(q.Get("sort"))); val != "" {
		sqlCol, ok := allowedColumns[val]
		if !ok {
			return SortParams{}, fmt.Errorf("invalid sort field: %s", val)
		}
		sortBy = sqlCol
	} else if mapped, ok := allowedColumns[defaultCol]; ok {
		sortBy = mapped
	}

	dir := strings.ToUpper(strings.TrimSpace(q.Get("order")))
	if dir == "" {
		dir = strings.ToUpper(strings.TrimSpace(q.Get("sort_dir")))
	}
	if dir == "" {
		dir = strings.ToUpper(strings.TrimSpace(q.Get("sort_direction")))
	}
	if dir == "" {
		dir = defaultDir
	}

	if dir != "ASC" && dir != "DESC" {
		return SortParams{}, fmt.Errorf("invalid sort direction: must be 'asc' or 'desc'")
	}

	return SortParams{
		SortBy:    sortBy,
		Direction: dir,
	}, nil
}
