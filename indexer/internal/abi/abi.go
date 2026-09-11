package abi

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// MainABI contains the ABI for the WTF MonthlyPayroll contract.
//
// This ABI is used in read-only mode for:
//   - identifying contract events
//   - decoding event logs
//   - decoding read-only contract method data
//
// No private key, signer, or transaction functionality is required.
const MainABI = "[{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_token\",\"type\":\"address\"}],\"stateMutability\":\"nonpayable\",\"type\":\"constructor\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"remianingTime\",\"type\":\"uint256\"}],\"name\":\"ClaimTooEarly\",\"type\":\"error\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"owner\",\"type\":\"address\"}],\"name\":\"OwnableInvalidOwner\",\"type\":\"error\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"account\",\"type\":\"address\"}],\"name\":\"OwnableUnauthorizedAccount\",\"type\":\"error\"},{\"inputs\":[],\"name\":\"ReentrancyGuardReentrantCall\",\"type\":\"error\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"token\",\"type\":\"address\"}],\"name\":\"SafeERC20FailedOperation\",\"type\":\"error\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"employee\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"salary\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"allocation\",\"type\":\"uint256\"}],\"name\":\"EmployeeAdded\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"employee\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"EmployeeRemoved\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"EmployerAdded\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"EmployerRemoved\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"previousOwner\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"newOwner\",\"type\":\"address\"}],\"name\":\"OwnershipTransferred\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"},{\"indexed\":true,\"internalType\":\"address\",\"name\":\"employee\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"amountPaid\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"fee\",\"type\":\"uint256\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"amountCredited\",\"type\":\"uint256\"}],\"name\":\"PayrollFunded\",\"type\":\"event\"},{\"anonymous\":false,\"inputs\":[{\"indexed\":true,\"internalType\":\"address\",\"name\":\"employee\",\"type\":\"address\"},{\"indexed\":false,\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"SalaryClaimed\",\"type\":\"event\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_employee\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"_employer\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"_salaryPerSec\",\"type\":\"uint256\"}],\"name\":\"addEmployee\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_wallet\",\"type\":\"address\"}],\"name\":\"addEmployer\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_employee\",\"type\":\"address\"}],\"name\":\"checkAccumulatedSalaryAmount\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"checkClaimableSalaryAmount\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"claimSalary\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"contractBalance\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"dailyPayrollCost\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"amount\",\"type\":\"uint256\"}],\"name\":\"emergencyWithdrawToken\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"employeeCount\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"employeeIndex\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"employees\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"wallet\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"salaryPerSecond\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"lastWithdraw\",\"type\":\"uint256\"},{\"internalType\":\"bool\",\"name\":\"active\",\"type\":\"bool\"},{\"internalType\":\"uint256\",\"name\":\"totalLeaves\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"deactivationTime\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"employerIndex\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"name\":\"employerList\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"employers\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"wallet\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"funds\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"totalSalaryPerSecond\",\"type\":\"uint256\"},{\"internalType\":\"bool\",\"name\":\"active\",\"type\":\"bool\"},{\"internalType\":\"uint256\",\"name\":\"deactivationTime\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_employee\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"_amount\",\"type\":\"uint256\"}],\"name\":\"fundPayroll\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_employer\",\"type\":\"address\"}],\"name\":\"fundingNeededPayrolls\",\"outputs\":[{\"internalType\":\"address[]\",\"name\":\"\",\"type\":\"address[]\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"fundingRequiredForThirtyDays\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getAllEmployers\",\"outputs\":[{\"internalType\":\"address[]\",\"name\":\"\",\"type\":\"address[]\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_wallet\",\"type\":\"address\"}],\"name\":\"getEmployee\",\"outputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"wallet\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"salaryPerSecond\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"lastWithdraw\",\"type\":\"uint256\"},{\"internalType\":\"bool\",\"name\":\"active\",\"type\":\"bool\"},{\"internalType\":\"uint256\",\"name\":\"totalLeaves\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"deactivationTime\",\"type\":\"uint256\"}],\"internalType\":\"structMonthlyPayroll.Employee\",\"name\":\"\",\"type\":\"tuple\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_employee\",\"type\":\"address\"}],\"name\":\"getEmployeePayrollBalance\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_employer\",\"type\":\"address\"}],\"name\":\"getEmployeesOfEmployer\",\"outputs\":[{\"internalType\":\"address[]\",\"name\":\"\",\"type\":\"address[]\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"getEmployer\",\"outputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"wallet\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"funds\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"totalSalaryPerSecond\",\"type\":\"uint256\"},{\"internalType\":\"bool\",\"name\":\"active\",\"type\":\"bool\"},{\"internalType\":\"uint256\",\"name\":\"deactivationTime\",\"type\":\"uint256\"}],\"internalType\":\"structMonthlyPayroll.Employer\",\"name\":\"\",\"type\":\"tuple\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getEmployerCount\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"getEmployerFunds\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"getEmployerSalaryPerSecond\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getMypayrollStreamBalance\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"getPlatformFee\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"hasThirtyDayRunway\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employee\",\"type\":\"address\"}],\"name\":\"isEmployeeActive\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"isEmployerActive\",\"outputs\":[{\"internalType\":\"bool\",\"name\":\"\",\"type\":\"bool\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"monthlyPayrollCost\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"myEmployer\",\"outputs\":[{\"components\":[{\"internalType\":\"address\",\"name\":\"wallet\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"salaryPerSecond\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"lastWithdraw\",\"type\":\"uint256\"},{\"internalType\":\"bool\",\"name\":\"active\",\"type\":\"bool\"},{\"internalType\":\"uint256\",\"name\":\"totalLeaves\",\"type\":\"uint256\"},{\"internalType\":\"uint256\",\"name\":\"deactivationTime\",\"type\":\"uint256\"}],\"internalType\":\"structMonthlyPayroll.Employee\",\"name\":\"\",\"type\":\"tuple\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"owner\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"payrollToken\",\"outputs\":[{\"internalType\":\"contractIERC20\",\"name\":\"\",\"type\":\"address\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"\",\"type\":\"address\"}],\"name\":\"payrolls\",\"outputs\":[{\"internalType\":\"address\",\"name\":\"employee\",\"type\":\"address\"},{\"internalType\":\"uint256\",\"name\":\"funds\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"platformFeePercent\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_employee\",\"type\":\"address\"},{\"internalType\":\"address\",\"name\":\"_employer\",\"type\":\"address\"}],\"name\":\"removeEmployee\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"_wallet\",\"type\":\"address\"}],\"name\":\"removeEmployer\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[],\"name\":\"renounceOwnership\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"employer\",\"type\":\"address\"}],\"name\":\"runway\",\"outputs\":[{\"internalType\":\"uint256\",\"name\":\"\",\"type\":\"uint256\"}],\"stateMutability\":\"view\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"uint256\",\"name\":\"fee\",\"type\":\"uint256\"}],\"name\":\"setPlatformFee\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"},{\"inputs\":[{\"internalType\":\"address\",\"name\":\"newOwner\",\"type\":\"address\"}],\"name\":\"transferOwnership\",\"outputs\":[],\"stateMutability\":\"nonpayable\",\"type\":\"function\"}]"

// MainFilterer provides read-only ABI decoding for MonthlyPayroll events.
type MainFilterer struct {
	abi      *abi.ABI
	contract *bind.BoundContract
}

// ABIEmployeeAddedEvent represents the decoded EmployeeAdded event.
type ABIEmployeeAddedEvent struct {
	Employee   common.Address
	Employer   common.Address
	Salary     *big.Int
	Allocation *big.Int
}

// ParseABIEmployeeAddedEvent decodes an EmployeeAdded log.
func (m *MainFilterer) ParseABIEmployeeAddedEvent(log types.Log) (*ABIEmployeeAddedEvent, error) {
	event := new(ABIEmployeeAddedEvent)

	if err := m.contract.UnpackLog(event, "EmployeeAdded", log); err != nil {
		return nil, err
	}

	return event, nil
}

// ABIEmployeeRemovedEvent represents the decoded EmployeeRemoved event.
type ABIEmployeeRemovedEvent struct {
	Employee common.Address
	Employer common.Address
}

// ParseABIEmployeeRemovedEvent decodes an EmployeeRemoved log.
func (m *MainFilterer) ParseABIEmployeeRemovedEvent(log types.Log) (*ABIEmployeeRemovedEvent, error) {
	event := new(ABIEmployeeRemovedEvent)

	if err := m.contract.UnpackLog(event, "EmployeeRemoved", log); err != nil {
		return nil, err
	}

	return event, nil
}

// ABIEmployerAddedEvent represents the decoded EmployerAdded event.
type ABIEmployerAddedEvent struct {
	Employer common.Address
}

// ParseABIEmployerAddedEvent decodes an EmployerAdded log.
func (m *MainFilterer) ParseABIEmployerAddedEvent(log types.Log) (*ABIEmployerAddedEvent, error) {
	event := new(ABIEmployerAddedEvent)

	if err := m.contract.UnpackLog(event, "EmployerAdded", log); err != nil {
		return nil, err
	}

	return event, nil
}

// ABIEmployerRemovedEvent represents the decoded EmployerRemoved event.
type ABIEmployerRemovedEvent struct {
	Employer common.Address
}

// ParseABIEmployerRemovedEvent decodes an EmployerRemoved log.
func (m *MainFilterer) ParseABIEmployerRemovedEvent(log types.Log) (*ABIEmployerRemovedEvent, error) {
	event := new(ABIEmployerRemovedEvent)

	if err := m.contract.UnpackLog(event, "EmployerRemoved", log); err != nil {
		return nil, err
	}

	return event, nil
}

// ABIOwnershipTransferredEvent represents the decoded OwnershipTransferred event.
type ABIOwnershipTransferredEvent struct {
	PreviousOwner common.Address
	NewOwner      common.Address
}

// ParseABIOwnershipTransferredEvent decodes an OwnershipTransferred log.
func (m *MainFilterer) ParseABIOwnershipTransferredEvent(log types.Log) (*ABIOwnershipTransferredEvent, error) {
	event := new(ABIOwnershipTransferredEvent)

	if err := m.contract.UnpackLog(event, "OwnershipTransferred", log); err != nil {
		return nil, err
	}

	return event, nil
}

// ABIPayrollFundedEvent represents the decoded PayrollFunded event.
type ABIPayrollFundedEvent struct {
	Employer       common.Address
	Employee       common.Address
	AmountPaid     *big.Int
	Fee            *big.Int
	AmountCredited *big.Int
}

// ParseABIPayrollFundedEvent decodes a PayrollFunded log.
func (m *MainFilterer) ParseABIPayrollFundedEvent(log types.Log) (*ABIPayrollFundedEvent, error) {
	event := new(ABIPayrollFundedEvent)

	if err := m.contract.UnpackLog(event, "PayrollFunded", log); err != nil {
		return nil, err
	}

	return event, nil
}

// ABISalaryClaimedEvent represents the decoded SalaryClaimed event.
type ABISalaryClaimedEvent struct {
	Employee common.Address
	Amount   *big.Int
}

// ParseABISalaryClaimedEvent decodes a SalaryClaimed log.
func (m *MainFilterer) ParseABISalaryClaimedEvent(log types.Log) (*ABISalaryClaimedEvent, error) {
	event := new(ABISalaryClaimedEvent)

	if err := m.contract.UnpackLog(event, "SalaryClaimed", log); err != nil {
		return nil, err
	}

	return event, nil
}

// NewMainFilterer creates a read-only MonthlyPayroll ABI decoder.
//
// No RPC connection, private key, signer, or transaction capability is
// required to decode logs.
func NewMainFilterer(address common.Address) (*MainFilterer, error) {
	parsedABI, err := abi.JSON(strings.NewReader(MainABI))
	if err != nil {
		return nil, err
	}

	// BoundContract only needs the ABI and contract address for UnpackLog.
	// caller, transactor and filterer are nil because this package is used
	// only for read-only ABI decoding.
	contract := bind.NewBoundContract(
		address,
		parsedABI,
		nil,
		nil,
		nil,
	)

	return &MainFilterer{
		abi:      &parsedABI,
		contract: contract,
	}, nil
}

// ABI returns the parsed contract ABI.
//
// This can be useful when another part of the indexer needs to inspect
// event or method definitions.
func (m *MainFilterer) ABI() *abi.ABI {
	return m.abi
}

// UnpackMethodIntoInterface decodes ABI-encoded method input data.
//
// This is read-only decoding. It does not execute or send a transaction.
func (m *MainFilterer) UnpackMethodIntoInterface(
	v interface{},
	name string,
	data []byte,
) error {
	method, ok := m.abi.Methods[name]
	if !ok {
		return abi.ErrMethodNotFound
	}

	if len(data) < 4 {
		return abi.ErrNoMethodID
	}

	unpacked, err := method.Inputs.Unpack(data[4:])
	if err != nil {
		return err
	}

	return method.Inputs.Copy(v, unpacked)
}