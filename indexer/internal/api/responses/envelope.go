package responses

import (
	"encoding/json"
	"net/http"
)

// SuccessEnvelope represents a single-resource success response.
type SuccessEnvelope struct {
	Data any `json:"data"`
	Meta any `json:"meta,omitempty"`
}

// ListEnvelope represents a list response with standard pagination metadata.
type ListEnvelope struct {
	Data       any             `json:"data"`
	Meta       PaginationMeta  `json:"meta"`
	Pagination *PaginationMeta `json:"pagination,omitempty"`
}

// PaginationMeta contains pagination details for list endpoints.
type PaginationMeta struct {
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
	Total    int64 `json:"total"`
	HasNext  bool  `json:"has_next"`
}

// ErrorDetail represents the structured error payload.
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ErrorEnvelope represents the standard error response.
type ErrorEnvelope struct {
	Error ErrorDetail `json:"error"`
}

// WriteJSON writes any payload as JSON with the specified status code.
func WriteJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}

// WriteSuccess writes a standard single-resource success envelope.
func WriteSuccess(w http.ResponseWriter, statusCode int, data any, meta any) {
	WriteJSON(w, statusCode, SuccessEnvelope{
		Data: data,
		Meta: meta,
	})
}

// WriteList writes a standard paginated list response envelope.
func WriteList(w http.ResponseWriter, data any, page int, pageSize int, total int64) {
	hasNext := (int64(page) * int64(pageSize)) < total
	pMeta := PaginationMeta{
		Page:     page,
		PageSize: pageSize,
		Total:    total,
		HasNext:  hasNext,
	}
	WriteJSON(w, http.StatusOK, ListEnvelope{
		Data:       data,
		Meta:       pMeta,
		Pagination: &pMeta,
	})
}

// WriteError writes a standard error envelope with the provided status code and error code.
func WriteError(w http.ResponseWriter, statusCode int, code string, message string) {
	WriteJSON(w, statusCode, ErrorEnvelope{
		Error: ErrorDetail{
			Code:    code,
			Message: message,
		},
	})
}
