package responses

// Stable machine-readable error codes as required by the API specification.
const (
	ErrCodeInvalidAddress         = "INVALID_ADDRESS"
	ErrCodeInvalidTransactionHash = "INVALID_TRANSACTION_HASH"
	ErrCodeTransactionNotFound    = "TRANSACTION_NOT_FOUND"
	ErrCodeEmployerNotFound       = "EMPLOYER_NOT_FOUND"
	ErrCodeEmployeeNotFound       = "EMPLOYEE_NOT_FOUND"
	ErrCodeInvalidDirection       = "INVALID_DIRECTION"
	ErrCodeInvalidBlockRange      = "INVALID_BLOCK_RANGE"
	ErrCodeInvalidDateRange       = "INVALID_DATE_RANGE"
	ErrCodeInvalidPagination      = "INVALID_PAGINATION"
	ErrCodeServiceNotReady        = "SERVICE_NOT_READY"
	ErrCodeUnauthorized           = "UNAUTHORIZED"
	ErrCodeInternalError          = "INTERNAL_SERVER_ERROR"
	ErrCodeBadRequest             = "BAD_REQUEST"
	ErrCodeNotImplemented         = "NOT_IMPLEMENTED"
)
