// Modifications Copyright 2026 The Kaia Authors
// This file is part of the Kaia library.
//
// The Kaia library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The Kaia library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the Kaia library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/kaiachain/kaia/blockchain/state"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/params"
	"github.com/kaiachain/kaia/storage/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockswordsCallDependencyTracer(t *testing.T) {
	contractAddr := common.HexToAddress("0xc0ffee")
	balanceTarget := common.HexToAddress("0xba1a11ce")
	slot := common.HexToHash("0x07")

	tr := NewBlockswordsCallDependencyTracer(nil)
	contract := NewContract(AccountRef(contractAddr), AccountRef(contractAddr), big.NewInt(0), 0, nil)

	capture := func(op OpCode, top *common.Hash) {
		st := newstack()
		if top != nil {
			var v uint256.Int
			v.SetBytes(top.Bytes())
			st.push(&v)
		}
		tr.CaptureState(nil, 0, op, 0, 0, 0, 0, &ScopeContext{Stack: st, Contract: contract}, 0, nil)
	}

	capture(SLOAD, &slot)                                      // storage dep
	balanceTopArg := common.BytesToHash(balanceTarget.Bytes()) // BALANCE reads addr from stack top
	capture(BALANCE, &balanceTopArg)                           // balance dep on target
	capture(SELFBALANCE, nil)                                  // balance dep on self
	capture(TIMESTAMP, nil)                                    // block context
	capture(NUMBER, nil)                                       // block context

	// Storage slot is recorded under the executing contract.
	foundSlot := false
	for _, tup := range tr.AccessList() {
		if tup.Address == contractAddr {
			for _, k := range tup.StorageKeys {
				if k == slot {
					foundSlot = true
				}
			}
		}
	}
	assert.True(t, foundSlot, "SLOAD slot must be in the access list")

	balances := tr.BalanceAddresses()
	assert.Contains(t, balances, balanceTarget, "BALANCE target must be a balance dep")
	assert.Contains(t, balances, contractAddr, "SELFBALANCE must record the executing contract")

	assert.False(t, tr.Trackable(), "a block-context read makes the call untrackable")
	assert.False(t, tr.StorageTrackable(), "block context and balance reads are not storage-trackable")
	bc := tr.BlockContextOpcodes()
	assert.Contains(t, bc, "TIMESTAMP")
	assert.Contains(t, bc, "NUMBER")
}

// TestBlockswordsCallDependencyTracerBalanceNotStorageTrackable is the
// regression for kaia_callWithAccessedStorage reporting trackable:true for a
// result that depends on an account balance (e.g. a liquid-staking exchange rate
// that reads the staking contract's native balance, which the reward distributor
// grows every block with no storage write). Such a result IS Trackable at
// account granularity (kaia_subscribe("callResults") observes the balance) but
// must NOT be StorageTrackable: a storage-only watch (kaia_subscribe(
// "storageChanges") fed the access list) cannot see the balance and would serve
// it stale.
func TestBlockswordsCallDependencyTracerBalanceNotStorageTrackable(t *testing.T) {
	tr := NewBlockswordsCallDependencyTracer(nil)
	addr := common.HexToAddress("0x9999")
	contract := NewContract(AccountRef(addr), AccountRef(addr), big.NewInt(0), 0, nil)

	st := newstack()
	var v uint256.Int
	v.SetBytes(common.HexToHash("0x03").Bytes())
	st.push(&v)
	tr.CaptureState(nil, 0, SLOAD, 0, 0, 0, 0, &ScopeContext{Stack: st, Contract: contract}, 0, nil)
	tr.CaptureState(nil, 0, SELFBALANCE, 0, 0, 0, 0, &ScopeContext{Stack: newstack(), Contract: contract}, 0, nil)

	assert.Empty(t, tr.BlockContextOpcodes(), "no block-context opcode was read")
	assert.Contains(t, tr.BalanceAddresses(), addr, "SELFBALANCE records a balance dependency")
	assert.True(t, tr.Trackable(), "a balance dep is trackable at account granularity")
	assert.False(t, tr.StorageTrackable(), "a balance dep is NOT trackable by a storage-only watch")
}

// TestBlockswordsCallDependencyTracerCodeNotStorageTrackable verifies that a
// code/existence read (EXTCODESIZE/EXTCODEHASH/EXTCODECOPY) is, like a balance
// read, NOT storage-trackable — a storage-slot watch cannot see a deployment,
// self-destruct, or EIP-7702 (re)delegation — yet IS trackable at account
// granularity.
func TestBlockswordsCallDependencyTracerCodeNotStorageTrackable(t *testing.T) {
	tr := NewBlockswordsCallDependencyTracer(nil)
	self := common.HexToAddress("0x01")
	target := common.HexToAddress("0xc0de")
	contract := NewContract(AccountRef(self), AccountRef(self), big.NewInt(0), 0, nil)

	st := newstack()
	var v uint256.Int
	v.SetBytes(target.Bytes()) // EXTCODESIZE reads the address from the stack top
	st.push(&v)
	tr.CaptureState(nil, 0, EXTCODESIZE, 0, 0, 0, 0, &ScopeContext{Stack: st, Contract: contract}, 0, nil)

	assert.Contains(t, tr.CodeAddresses(), target, "EXTCODESIZE records a code/existence dependency")
	assert.Empty(t, tr.BalanceAddresses())
	assert.Empty(t, tr.BlockContextOpcodes())
	assert.True(t, tr.Trackable(), "a code dep is trackable at account granularity")
	assert.False(t, tr.StorageTrackable(), "a code dep is NOT trackable by a storage-only watch")
}

// TestBlockswordsCallDependencyTracerGasPrice verifies GASPRICE is classified as
// block context: on Kaia the effective gas price defaults to baseFee*2 when the
// caller omits it, so it is block-varying like BASEFEE and a result reading it is
// neither Trackable nor StorageTrackable.
func TestBlockswordsCallDependencyTracerGasPrice(t *testing.T) {
	tr := NewBlockswordsCallDependencyTracer(nil)
	self := common.HexToAddress("0x01")
	contract := NewContract(AccountRef(self), AccountRef(self), big.NewInt(0), 0, nil)

	tr.CaptureState(nil, 0, GASPRICE, 0, 0, 0, 0, &ScopeContext{Stack: newstack(), Contract: contract}, 0, nil)

	assert.Contains(t, tr.BlockContextOpcodes(), "GASPRICE")
	assert.False(t, tr.Trackable(), "a GASPRICE read makes the call untrackable")
	assert.False(t, tr.StorageTrackable())
}

func TestBlockswordsCallDependencyTracerTrackable(t *testing.T) {
	tr := NewBlockswordsCallDependencyTracer(nil)
	addr := common.HexToAddress("0x01")
	contract := NewContract(AccountRef(addr), AccountRef(addr), big.NewInt(0), 0, nil)

	st := newstack()
	var v uint256.Int
	v.SetBytes(common.HexToHash("0x05").Bytes())
	st.push(&v)
	tr.CaptureState(nil, 0, SLOAD, 0, 0, 0, 0, &ScopeContext{Stack: st, Contract: contract}, 0, nil)

	assert.True(t, tr.Trackable(), "a pure storage read is trackable")
	assert.True(t, tr.StorageTrackable(), "a pure storage read is storage-trackable")
	assert.Empty(t, tr.BalanceAddresses())
	assert.Empty(t, tr.BlockContextOpcodes())
}

// TestBlockswordsCallDependencyTracerCrossFrame proves that block-context
// detection traverses nested call frames exactly as storage-access capture does:
// both run off the same per-opcode CaptureState, which the interpreter invokes
// at every depth. It refutes the hypothesis that a TIMESTAMP read inside a
// nested STATICCALL goes undetected — here the outer contract ONLY STATICCALLs
// an inner contract, and it is the inner frame that reads TIMESTAMP and SLOADs a
// slot; both must surface on the root tracer.
func TestBlockswordsCallDependencyTracerCrossFrame(t *testing.T) {
	innerAddr := common.HexToAddress("0x1111")
	outerAddr := common.HexToAddress("0x2222")
	caller := common.HexToAddress("0x3333")
	innerSlot := common.HexToHash("0x07")

	// inner: TIMESTAMP; POP; PUSH1 0x07; SLOAD; POP; STOP
	innerCode := []byte{0x42, 0x50, 0x60, 0x07, 0x54, 0x50, 0x00}
	// outer: STATICCALL(gas, innerAddr, 0, 0, 0, 0); STOP
	// Push args deepest-first: retSize, retOffset, argsSize, argsOffset, addr, gas.
	outerCode := []byte{
		0x60, 0x00, // PUSH1 0  (retSize)
		0x60, 0x00, // PUSH1 0  (retOffset)
		0x60, 0x00, // PUSH1 0  (argsSize)
		0x60, 0x00, // PUSH1 0  (argsOffset)
		0x73, // PUSH20 innerAddr
	}
	outerCode = append(outerCode, innerAddr.Bytes()...)
	outerCode = append(outerCode, 0x5a, 0xfa, 0x00) // GAS; STATICCALL; STOP

	statedb, _ := state.New(common.Hash{}, state.NewDatabase(database.NewMemoryDBManager()), nil, nil)
	statedb.CreateSmartContractAccount(innerAddr, params.CodeFormatEVM, params.Rules{})
	require.NoError(t, statedb.SetCode(innerAddr, innerCode))
	statedb.CreateSmartContractAccount(outerAddr, params.CodeFormatEVM, params.Rules{})
	require.NoError(t, statedb.SetCode(outerAddr, outerCode))

	blockCtx := BlockContext{
		CanTransfer: func(StateDB, common.Address, *big.Int) bool { return true },
		Transfer:    func(StateDB, common.Address, common.Address, *big.Int) {},
		BlockNumber: big.NewInt(1),
		Time:        big.NewInt(1_700_000_000), // TIMESTAMP dereferences this
		BaseFee:     big.NewInt(0),
	}
	tracer := NewBlockswordsCallDependencyTracer(nil)
	vmCfg := &Config{Debug: true, Tracer: tracer, ComputationCostLimit: params.OpcodeComputationCostLimitInfinite}
	env := NewEVM(blockCtx, TxContext{}, statedb, params.TestChainConfig, vmCfg)

	_, _, err := env.Call(AccountRef(caller), outerAddr, nil, math.MaxUint64, big.NewInt(0))
	assert.NoError(t, err)

	// The block-context read happened only in the NESTED frame, yet it is on the
	// root tracer — so trackability correctly accounts for it.
	assert.Contains(t, tracer.BlockContextOpcodes(), "TIMESTAMP",
		"a TIMESTAMP read inside a nested STATICCALL must be detected")
	assert.False(t, tracer.Trackable(), "a nested block-context read makes the call untrackable")
	assert.False(t, tracer.StorageTrackable())

	// The nested frame's SLOAD slot is captured by the very same mechanism,
	// confirming both classifications are cross-frame.
	foundSlot := false
	for _, tup := range tracer.AccessList() {
		if tup.Address == innerAddr {
			for _, k := range tup.StorageKeys {
				if k == innerSlot {
					foundSlot = true
				}
			}
		}
	}
	assert.True(t, foundSlot, "a SLOAD inside the nested frame must be in the access list")
}

// TestBlockswordsCallDependencyTracerValidateSenderKey verifies that a call into
// the Kaia validateSender precompile records the validated account (its `from`
// input) as a key dependency — an account-granularity dep no opcode exposes, so
// callResults re-evaluates when that account's key changes (AccountUpdate).
func TestBlockswordsCallDependencyTracerValidateSenderKey(t *testing.T) {
	tr := NewBlockswordsCallDependencyTracer(nil)
	signer := common.HexToAddress("0x5161")
	caller := common.HexToAddress("0xca11e7")

	statedb, _ := state.New(common.Hash{}, state.NewDatabase(database.NewMemoryDBManager()), nil, nil)
	env := NewEVM(BlockContext{BlockNumber: big.NewInt(1), Time: big.NewInt(1)}, TxContext{}, statedb, params.TestChainConfig, &Config{})

	// Resolve the validateSender precompile address active for this config rather
	// than hardcoding it, so the test tracks the same address the tracer resolves.
	var vsAddr common.Address
	for addr, p := range env.GetPrecompiledContractMap(caller) {
		if _, ok := p.(*validateSender); ok {
			vsAddr = addr
		}
	}
	require.NotEqual(t, common.Address{}, vsAddr, "validateSender precompile must be active in the test config")

	tr.CaptureStart(env, caller, common.HexToAddress("0xdead"), false, nil, 0, nil) // captures env
	// validateSender input = from(20) ‖ msg(32) ‖ sig(65); only `from` matters.
	input := append(signer.Bytes(), make([]byte, common.HashLength+common.SignatureLength)...)
	tr.CaptureEnter(STATICCALL, caller, vsAddr, input, 0, nil)

	assert.Contains(t, tr.KeyAddresses(), signer, "validateSender records its `from` as a key dependency")
	assert.Empty(t, tr.BlockContextOpcodes())
	assert.True(t, tr.Trackable(), "a key dep is trackable at account granularity")
	assert.False(t, tr.StorageTrackable(), "a key dep is NOT trackable by a storage-only watch")
}

// TestBlockswordsCallDependencyTracerCreate2Existence verifies that entering a
// CREATE/CREATE2 frame records the would-be created address: the collision check
// reads its prior occupancy, so the result can depend on whether it is taken.
func TestBlockswordsCallDependencyTracerCreate2Existence(t *testing.T) {
	tr := NewBlockswordsCallDependencyTracer(nil)
	created := common.HexToAddress("0xc4ea7ed")

	tr.CaptureEnter(CREATE2, common.HexToAddress("0xfac7027"), created, nil, 0, nil)

	assert.Contains(t, tr.CodeAddresses(), created, "a CREATE2 created address is a code/existence dependency")
	assert.False(t, tr.StorageTrackable(), "a create-existence dep is NOT storage-trackable")
}

// TestBlockswordsCallDependencyTracerDelegation verifies that entering a call
// frame to an EIP-7702 delegated account records the delegation target as a code
// dependency — executing the account runs the target's code, so the result
// depends on it.
func TestBlockswordsCallDependencyTracerDelegation(t *testing.T) {
	tr := NewBlockswordsCallDependencyTracer(nil)
	eoa := common.HexToAddress("0xe0a")      // 7702-delegated account
	target := common.HexToAddress("0x7a6e7") // delegation target

	statedb, _ := state.New(common.Hash{}, state.NewDatabase(database.NewMemoryDBManager()), nil, nil)
	statedb.CreateSmartContractAccount(eoa, params.CodeFormatEVM, params.Rules{})
	require.NoError(t, statedb.SetCode(eoa, types.AddressToDelegation(target)))

	blockCtx := BlockContext{
		CanTransfer: func(StateDB, common.Address, *big.Int) bool { return true },
		Transfer:    func(StateDB, common.Address, common.Address, *big.Int) {},
		BlockNumber: big.NewInt(1),
		Time:        big.NewInt(1),
	}
	env := NewEVM(blockCtx, TxContext{}, statedb, params.TestChainConfig, &Config{})

	tr.CaptureStart(env, common.Address{}, eoa, false, nil, 0, nil)

	assert.Contains(t, tr.CodeAddresses(), target, "an EIP-7702 delegation target is a code dependency")
	assert.True(t, tr.Trackable(), "a delegation-target dep is trackable at account granularity")
	assert.False(t, tr.StorageTrackable(), "a delegation-target dep is NOT storage-trackable")
}
