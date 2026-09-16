package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"worldtradefuture/indexer/internal/models"
)

type ReconciliationFilter struct {
	Severity      *string
	Status        *string
	Type          *string
	FromDate      *time.Time
	ToDate        *time.Time
	Limit         int
	Offset        int
	SortBy        string
	SortDirection string
}

type ReconciliationRepository struct {
	pool *pgxpool.Pool
}

func NewReconciliationRepository(pool *pgxpool.Pool) *ReconciliationRepository {
	return &ReconciliationRepository{pool: pool}
}

// ListExceptions queries reconciliation exceptions with dynamic filters, total count, and pagination.
func (r *ReconciliationRepository) ListExceptions(ctx context.Context, filter ReconciliationFilter) ([]*models.ReconciliationException, int64, error) {
	var whereClauses []string
	var args []any
	argIdx := 1

	if filter.Severity != nil && *filter.Severity != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("LOWER(severity) = LOWER($%d)", argIdx))
		args = append(args, *filter.Severity)
		argIdx++
	}

	if filter.Status != nil && *filter.Status != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("LOWER(status) = LOWER($%d)", argIdx))
		args = append(args, *filter.Status)
		argIdx++
	}

	if filter.Type != nil && *filter.Type != "" {
		whereClauses = append(whereClauses, fmt.Sprintf("LOWER(type) = LOWER($%d)", argIdx))
		args = append(args, *filter.Type)
		argIdx++
	}

	if filter.FromDate != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("detected_at >= $%d", argIdx))
		args = append(args, filter.FromDate.UTC())
		argIdx++
	}

	if filter.ToDate != nil {
		whereClauses = append(whereClauses, fmt.Sprintf("detected_at <= $%d", argIdx))
		args = append(args, filter.ToDate.UTC())
		argIdx++
	}

	whereSQL := "1=1"
	if len(whereClauses) > 0 {
		whereSQL = strings.Join(whereClauses, " AND ")
	}

	// 1. Get count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM reconciliation_exceptions WHERE %s", whereSQL)
	var total int64
	err := r.pool.QueryRow(ctx, countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count reconciliation exceptions: %w", err)
	}

	// 2. Fetch data
	sortBy := "detected_at"
	switch filter.SortBy {
	case "detected_at", "severity", "status":
		sortBy = filter.SortBy
	}

	sortDir := "DESC"
	if strings.EqualFold(filter.SortDirection, "ASC") {
		sortDir = "ASC"
	}

	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	dataQuery := fmt.Sprintf(`
		SELECT
			id,
			type,
			severity,
			entity_ref,
			expected,
			observed,
			status,
			detected_at,
			resolved_at
		FROM reconciliation_exceptions
		WHERE %s
		ORDER BY %s %s, id DESC
		LIMIT $%d OFFSET $%d
	`, whereSQL, sortBy, sortDir, argIdx, argIdx+1)

	args = append(args, limit, offset)

	rows, err := r.pool.Query(ctx, dataQuery, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query reconciliation exceptions: %w", err)
	}
	defer rows.Close()

	exceptions := make([]*models.ReconciliationException, 0)
	for rows.Next() {
		var (
			id         int64
			excType    string
			severity   string
			entityRef  string
			expected   []byte
			observed   []byte
			status     string
			detectedAt time.Time
			resolvedAt sql.NullTime
		)

		if err := rows.Scan(&id, &excType, &severity, &entityRef, &expected, &observed, &status, &detectedAt, &resolvedAt); err != nil {
			return nil, 0, fmt.Errorf("failed to scan reconciliation exception row: %w", err)
		}

		exc := &models.ReconciliationException{
			ID:         id,
			Type:       excType,
			Severity:   severity,
			EntityRef:  entityRef,
			Expected:   json.RawMessage(expected),
			Observed:   json.RawMessage(observed),
			Status:     status,
			DetectedAt: detectedAt.UTC(),
		}
		if resolvedAt.Valid {
			t := resolvedAt.Time.UTC()
			exc.ResolvedAt = &t
		}

		exceptions = append(exceptions, exc)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("error reading reconciliation exception rows: %w", err)
	}

	return exceptions, total, nil
}
