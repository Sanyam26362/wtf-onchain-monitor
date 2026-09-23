package models

import "time"

// AlchemyWebhookPayload represents the top-level payload sent by Alchemy Notify webhooks.
type AlchemyWebhookPayload struct {
	WebhookID string       `json:"webhookId"`
	ID        string       `json:"id"`
	CreatedAt time.Time    `json:"createdAt"`
	Type      string       `json:"type"`
	Event     AlchemyEvent `json:"event"`
}

// AlchemyEvent represents the event container in an Alchemy webhook.
type AlchemyEvent struct {
	Data AlchemyData `json:"data"`
}

// AlchemyData represents the block and transaction event data.
type AlchemyData struct {
	Block AlchemyBlock `json:"block"`
}

// AlchemyBlock encapsulates block metadata and logs captured by the webhook filter.
type AlchemyBlock struct {
	Number    string       `json:"number"`
	Hash      string       `json:"hash"`
	Timestamp string       `json:"timestamp"`
	Logs      []AlchemyLog `json:"logs"`
}

// AlchemyLog represents an Ethereum log entry delivered in the webhook block.
type AlchemyLog struct {
	TransactionHash  string   `json:"transactionHash"`
	LogIndex         int      `json:"logIndex"`
	TransactionIndex int      `json:"transactionIndex"`
	BlockNumber      string   `json:"blockNumber"`
	Address          string   `json:"address"`
	Data             string   `json:"data"`
	Topics           []string `json:"topics"`
	Account          struct {
		Address string `json:"address"`
	} `json:"account"`
}

// ChainSettledMessage is the event broadcast payload published to Redis Pub/Sub channel wtf:chain:settled.
type ChainSettledMessage struct {
	EscrowID string `json:"escrowId"`
	TxHash   string `json:"txHash"`
}
