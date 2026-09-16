package models

// StreamStatus represents the sync status of an indexer stream.
type StreamStatus struct {
	StreamID         string  `json:"stream_id"`
	Token            *string `json:"token,omitempty"`
	LastIndexedBlock uint64  `json:"last_indexed_block"`
	Lag              int64   `json:"lag"`
	Status           string  `json:"status"`
}

// SyncStatusResponse represents the data payload for GET /v1/sync/status.
type SyncStatusResponse struct {
	ChainID     int64           `json:"chain_id"`
	LatestBlock uint64          `json:"latest_block"`
	SafeBlock   uint64          `json:"safe_block"`
	Streams     []*StreamStatus `json:"streams"`
	LastError   *string         `json:"last_error"`
}
