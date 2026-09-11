package decoder

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"worldtradefuture/indexer/internal/abi"
)

// EventType identifies the type of MonthlyPayroll event.
type EventType string

const (
	EventEmployerAdded   EventType = "EmployerAdded"
	EventEmployerRemoved EventType = "EmployerRemoved"
	EventEmployeeAdded   EventType = "EmployeeAdded"
	EventEmployeeRemoved EventType = "EmployeeRemoved"
	EventPayrollFunded   EventType = "PayrollFunded"
	EventSalaryClaimed   EventType = "SalaryClaimed"
)

// DecodedEvent contains the event type, the decoded event data,
// and the original Ethereum log.
//
// The original log is retained because the indexer will later need
// blockchain provenance such as block number, transaction hash,
// transaction index, block hash, and log index.
type DecodedEvent struct {
	Type EventType
	Data interface{}
	Log  types.Log
}

// Decoder is responsible only for identifying and decoding
// MonthlyPayroll contract events.
type Decoder struct {
	filterer *abi.MainFilterer
}

// New creates a new MonthlyPayroll event decoder.
//
// The contract address is used when creating the ABI filterer.
// The decoder itself does not make an RPC call.
func New(contractAddress common.Address) (*Decoder, error) {
	filterer, err := abi.NewMainFilterer(contractAddress)
	if err != nil {
		return nil, fmt.Errorf("create ABI filterer: %w", err)
	}

	return &Decoder{
		filterer: filterer,
	}, nil
}

// Decode identifies and decodes a MonthlyPayroll event from
// an Ethereum log.
//
// The first topic (Topics[0]) contains the event signature hash.
// The remaining topics and Data contain the event parameters.
func (d *Decoder) Decode(log types.Log) (*DecodedEvent, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("log has no topics")
	}

	eventID := log.Topics[0]

	switch eventID {
	case d.filterer.ABI().Events["EmployerAdded"].ID:
		event, err := d.filterer.ParseABIEmployerAddedEvent(log)
		if err != nil {
			return nil, fmt.Errorf("decode EmployerAdded: %w", err)
		}

		return &DecodedEvent{
			Type: EventEmployerAdded,
			Data: event,
			Log:  log,
		}, nil

	case d.filterer.ABI().Events["EmployerRemoved"].ID:
		event, err := d.filterer.ParseABIEmployerRemovedEvent(log)
		if err != nil {
			return nil, fmt.Errorf("decode EmployerRemoved: %w", err)
		}

		return &DecodedEvent{
			Type: EventEmployerRemoved,
			Data: event,
			Log:  log,
		}, nil

	case d.filterer.ABI().Events["EmployeeAdded"].ID:
		event, err := d.filterer.ParseABIEmployeeAddedEvent(log)
		if err != nil {
			return nil, fmt.Errorf("decode EmployeeAdded: %w", err)
		}

		return &DecodedEvent{
			Type: EventEmployeeAdded,
			Data: event,
			Log:  log,
		}, nil

	case d.filterer.ABI().Events["EmployeeRemoved"].ID:
		event, err := d.filterer.ParseABIEmployeeRemovedEvent(log)
		if err != nil {
			return nil, fmt.Errorf("decode EmployeeRemoved: %w", err)
		}

		return &DecodedEvent{
			Type: EventEmployeeRemoved,
			Data: event,
			Log:  log,
		}, nil

	case d.filterer.ABI().Events["PayrollFunded"].ID:
		event, err := d.filterer.ParseABIPayrollFundedEvent(log)
		if err != nil {
			return nil, fmt.Errorf("decode PayrollFunded: %w", err)
		}

		return &DecodedEvent{
			Type: EventPayrollFunded,
			Data: event,
			Log:  log,
		}, nil

	case d.filterer.ABI().Events["SalaryClaimed"].ID:
		event, err := d.filterer.ParseABISalaryClaimedEvent(log)
		if err != nil {
			return nil, fmt.Errorf("decode SalaryClaimed: %w", err)
		}

		return &DecodedEvent{
			Type: EventSalaryClaimed,
			Data: event,
			Log:  log,
		}, nil

	default:
		return nil, fmt.Errorf(
			"unsupported MonthlyPayroll event topic: %s",
			eventID.Hex(),
		)
	}
}