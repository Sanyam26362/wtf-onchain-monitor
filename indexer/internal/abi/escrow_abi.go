package abi

import (
	"github.com/ethereum/go-ethereum/common"
)

// WTFEscrowABI is the canonical ABI JSON for the deployed WTFEscrow contract on Ethereum Sepolia.
// Target Contract: 0x807EB6317FbdF219C18B58ac0BF941bC4af268D5 (Chain ID: 11155111)
// Contains the 6 verified canonical events:
// 1. EscrowCreated(uint256 indexed escrowId, address indexed buyer, address indexed seller, uint256 amount)
// 2. EscrowReleased(uint256 indexed escrowId, uint256 amount)
// 3. EscrowRefunded(uint256 indexed escrowId, uint256 amount)
// 4. DisputeResolved(uint256 indexed escrowId, address indexed winner, uint256 amountReleased)
// 5. DeliveryAcknowledged(uint256 indexed escrowId, uint256 deliveryTime)
// 6. DisputeRaised(uint256 indexed escrowId, address indexed raisedBy, uint256 disputeFee)

const WTFEscrowABI = `[
  {
    "type": "event",
    "name": "EscrowCreated",
    "inputs": [
      {"name": "escrowId", "type": "uint256", "indexed": true},
      {"name": "buyer",    "type": "address", "indexed": true},
      {"name": "seller",   "type": "address", "indexed": true},
      {"name": "amount",   "type": "uint256", "indexed": false}
    ],
    "anonymous": false
  },
  {
    "type": "event",
    "name": "EscrowReleased",
    "inputs": [
      {"name": "escrowId", "type": "uint256", "indexed": true},
      {"name": "amount",   "type": "uint256", "indexed": false}
    ],
    "anonymous": false
  },
  {
    "type": "event",
    "name": "EscrowRefunded",
    "inputs": [
      {"name": "escrowId", "type": "uint256", "indexed": true},
      {"name": "amount",   "type": "uint256", "indexed": false}
    ],
    "anonymous": false
  },
  {
    "type": "event",
    "name": "DisputeResolved",
    "inputs": [
      {"name": "escrowId",       "type": "uint256", "indexed": true},
      {"name": "winner",         "type": "address", "indexed": true},
      {"name": "amountReleased", "type": "uint256", "indexed": false}
    ],
    "anonymous": false
  },
  {
    "type": "event",
    "name": "DeliveryAcknowledged",
    "inputs": [
      {"name": "escrowId",     "type": "uint256", "indexed": true},
      {"name": "deliveryTime", "type": "uint256", "indexed": false}
    ],
    "anonymous": false
  },
  {
    "type": "event",
    "name": "DisputeRaised",
    "inputs": [
      {"name": "escrowId",    "type": "uint256", "indexed": true},
      {"name": "raisedBy",    "type": "address", "indexed": true},
      {"name": "disputeFee",  "type": "uint256", "indexed": false}
    ],
    "anonymous": false
  }
]`

var (
	TopicEscrowCreated        = common.HexToHash("0x9405ad0a6208539879349284d71265479b1623846f70303da1f9890d6e8c10a7")
	TopicEscrowReleased       = common.HexToHash("0x10ce17ae7e78eb775b13182ea618b201c2c81afc8fee55c287291f8686f17eac")
	TopicEscrowRefunded       = common.HexToHash("0x8e09fed7623ad9f8d5a701dd9f3233197147d0043c564dfb1a6c0023ab568c20")
	TopicDisputeResolved      = common.HexToHash("0xdf0baf5489595ea17a097d16827d60793e2f57f3b85a982e5d7e02838b841ee6")
	TopicDeliveryAcknowledged = common.HexToHash("0x56f374d542ae03669d32ab9defbebfc87cfe94f6cf83f46cdb60822b744af892")
	TopicDisputeRaised        = common.HexToHash("0xe0e58e9cc5ed7ef092d3e14cff6136d23ebf75a16cf3c8257ef8804811d8b0f3")
)

// IsKnownEscrowTopic evaluates whether a given log topic0 matches any of the canonical WTFEscrow events.
func IsKnownEscrowTopic(topic common.Hash) bool {
	switch topic {
	case TopicEscrowCreated,
		TopicEscrowReleased,
		TopicEscrowRefunded,
		TopicDisputeResolved,
		TopicDeliveryAcknowledged,
		TopicDisputeRaised:
		return true
	default:
		return false
	}
}
