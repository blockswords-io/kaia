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

// Blockswords fork-only call-dependency tracer is kept in this file to minimize
// merge conflicts with upstream Kaia files.

import (
	"math/big"

	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
)

// BlockswordsCallDependencyTracer extends AccessListTracer with the extra
// dependency classification the reactive-call ("live kaia_call") engine needs.
// Beyond the storage slots and touched addresses the embedded AccessListTracer
// records (exactly as for eth_createAccessList), it classifies, per opcode:
//
//   - Balance reads (BALANCE / SELFBALANCE): the result depends on account
//     balances, which change outside the storage trie and so must be watched at
//     account granularity, not via storage slots.
//   - Code/existence reads (EXTCODESIZE / EXTCODEHASH / EXTCODECOPY): the result
//     depends on whether an account has code and on that code — also an
//     account-granularity dependency, not a storage slot.
//   - Block-context reads (TIMESTAMP, NUMBER, BASEFEE, GASPRICE, ...): a result
//     that reads these is not a pure function of state, so the engine
//     re-evaluates it every block instead of only on state changes.
//
// and, per call frame entered — three account-granularity dependencies that no
// opcode exposes, so they would otherwise be invisible:
//
//   - The account a validateSender precompile call validates (its `from` input):
//     the result reflects that account's key, which changes on an AccountUpdate.
//   - The target of an EIP-7702 delegation the call invokes: executing a
//     delegated account runs the target's code, so the result depends on it.
//   - The address a CREATE/CREATE2 would deploy to: the collision check reads its
//     prior occupancy, so the result can depend on whether it is already taken.
type BlockswordsCallDependencyTracer struct {
	*AccessListTracer
	balanceAddrs    map[common.Address]struct{}
	codeAddrs       map[common.Address]struct{}
	keyAddrs        map[common.Address]struct{}
	blockContextOps map[OpCode]struct{}
	// env is captured at CaptureStart so the call-frame hooks can resolve, in any
	// frame, whether a target is the validateSender precompile (for the caller's
	// VM version) and whether it is an EIP-7702 delegation (reading its code).
	env *EVM
}

// NewBlockswordsCallDependencyTracer creates a dependency tracer. Pass nil
// addressesToExclude to record every touched address.
func NewBlockswordsCallDependencyTracer(addressesToExclude map[common.Address]struct{}) *BlockswordsCallDependencyTracer {
	return &BlockswordsCallDependencyTracer{
		AccessListTracer: NewAccessListTracer(nil, addressesToExclude),
		balanceAddrs:     make(map[common.Address]struct{}),
		codeAddrs:        make(map[common.Address]struct{}),
		keyAddrs:         make(map[common.Address]struct{}),
		blockContextOps:  make(map[OpCode]struct{}),
	}
}

// CaptureState records storage/address access via the embedded AccessListTracer,
// then additionally classifies balance, code/existence, and block-context reads.
func (t *BlockswordsCallDependencyTracer) CaptureState(env *EVM, pc uint64, op OpCode, gas, cost, ccLeft, ccOpcode uint64, scope *ScopeContext, depth int, err error) {
	t.AccessListTracer.CaptureState(env, pc, op, gas, cost, ccLeft, ccOpcode, scope, depth, err)

	switch op {
	case BALANCE:
		stackData := scope.Stack.Data()
		if len(stackData) >= 1 {
			t.balanceAddrs[common.Address(stackData[len(stackData)-1].Bytes20())] = struct{}{}
		}
	case SELFBALANCE:
		t.balanceAddrs[scope.Contract.Address()] = struct{}{}
	case EXTCODESIZE, EXTCODEHASH, EXTCODECOPY:
		// These read an account's code/existence (address is the top stack item),
		// which a storage-slot watch cannot observe — track it like a balance dep.
		stackData := scope.Stack.Data()
		if len(stackData) >= 1 {
			t.codeAddrs[common.Address(stackData[len(stackData)-1].Bytes20())] = struct{}{}
		}
	case TIMESTAMP, NUMBER, BLOCKHASH, COINBASE, GASLIMIT, DIFFICULTY, BASEFEE, BLOBBASEFEE:
		t.blockContextOps[op] = struct{}{}
	case GASPRICE:
		// On Kaia the effective gas price defaults to baseFee*2 when the caller
		// specifies neither gasPrice nor maxFeePerGas (CallArgs.ToMessage), so
		// GASPRICE is block-varying like BASEFEE. Classify it as block context so
		// a result reading it is re-evaluated per block, not assumed state-pure.
		t.blockContextOps[op] = struct{}{}
	}
}

// CaptureStart captures the EVM (so the call-frame hooks can resolve precompiles
// and delegations) and classifies the top frame's call-frame dependencies.
func (t *BlockswordsCallDependencyTracer) CaptureStart(env *EVM, from, to common.Address, create bool, input []byte, gas uint64, value *big.Int) {
	t.AccessListTracer.CaptureStart(env, from, to, create, input, gas, value)
	t.env = env
	if create {
		t.recordCreatedAddr(to)
	} else {
		t.recordCallFrameDeps(from, to, input)
	}
}

// CaptureEnter classifies each nested call frame's call-frame dependencies.
// SELFDESTRUCT also enters here (its beneficiary value transfer) but executes no
// code and reads no key, so it introduces no out-of-band dependency and is
// ignored.
func (t *BlockswordsCallDependencyTracer) CaptureEnter(typ OpCode, from, to common.Address, input []byte, gas uint64, value *big.Int) {
	t.AccessListTracer.CaptureEnter(typ, from, to, input, gas, value)
	switch typ {
	case CREATE, CREATE2:
		t.recordCreatedAddr(to)
	case CALL, CALLCODE, DELEGATECALL, STATICCALL:
		t.recordCallFrameDeps(from, to, input)
	}
}

// recordCreatedAddr records a CREATE/CREATE2 would-be address. The collision
// check reads its prior occupancy (evm.create), so the result can depend on
// whether that address is already taken.
func (t *BlockswordsCallDependencyTracer) recordCreatedAddr(addr common.Address) {
	t.codeAddrs[addr] = struct{}{}
}

// recordCallFrameDeps records the account-granularity dependencies a CALL-family
// frame introduces that no opcode exposes: the account key a validateSender call
// validates, and the code behind an EIP-7702 delegation.
func (t *BlockswordsCallDependencyTracer) recordCallFrameDeps(from, to common.Address, input []byte) {
	if t.env == nil {
		return
	}
	// Resolve the precompile set exactly as execution does (keyed on the caller's
	// VM version), so this can't misclassify validateSender vs. another precompile
	// sharing an address in a newer VM version (e.g. bls12381G1Add at 0x0b).
	if _, ok := t.env.getPrecompiledContractForVersion(from)[to].(*validateSender); ok {
		// validateSender(from‖msg‖sigs) reports whether the signatures match
		// `from`'s account key; the result depends on that key. An AccountUpdate
		// changing the key dirties the account, so the account watch observes it.
		if len(input) >= common.AddressLength {
			t.keyAddrs[common.BytesToAddress(input[:common.AddressLength])] = struct{}{}
		}
		return
	}
	// Calling an EIP-7702 delegated account executes the delegation target's code,
	// so the result depends on that target (watched at account granularity, which
	// covers code), as resolveCode in evm.go resolves the same designator. The
	// parse is unconditional but harmless pre-Prague: the 0xef0100 designator form
	// cannot appear as account code before EIP-7702 exists.
	if target, ok := types.ParseDelegation(t.env.StateDB.GetCode(to)); ok {
		t.codeAddrs[target] = struct{}{}
	}
}

// BalanceAddresses returns the addresses whose balance the call read. These are
// account-granularity dependencies for the reactive-call engine.
func (t *BlockswordsCallDependencyTracer) BalanceAddresses() []common.Address {
	out := make([]common.Address, 0, len(t.balanceAddrs))
	for addr := range t.balanceAddrs {
		out = append(out, addr)
	}
	return out
}

// CodeAddresses returns the addresses whose code or existence the call read
// (EXTCODESIZE/EXTCODEHASH/EXTCODECOPY). Like balances, code/existence changes
// happen outside the storage trie, so these are account-granularity
// dependencies: they make a result un-StorageTrackable but still Trackable.
func (t *BlockswordsCallDependencyTracer) CodeAddresses() []common.Address {
	out := make([]common.Address, 0, len(t.codeAddrs))
	for addr := range t.codeAddrs {
		out = append(out, addr)
	}
	return out
}

// KeyAddresses returns the accounts whose key the call's result depends on,
// i.e. the `from` of every validateSender precompile call. An account key
// changes on an AccountUpdate (outside the storage trie), so these are
// account-granularity dependencies, like BalanceAddresses.
func (t *BlockswordsCallDependencyTracer) KeyAddresses() []common.Address {
	out := make([]common.Address, 0, len(t.keyAddrs))
	for addr := range t.keyAddrs {
		out = append(out, addr)
	}
	return out
}

// BlockContextOpcodes returns the names of the block-context opcodes the call
// used. A non-empty result means the call's output may change every block
// independently of state and therefore cannot be tracked precisely.
func (t *BlockswordsCallDependencyTracer) BlockContextOpcodes() []string {
	out := make([]string, 0, len(t.blockContextOps))
	for op := range t.blockContextOps {
		out = append(out, op.String())
	}
	return out
}

// Trackable reports whether the call's result is a pure function of state
// (storage + balances) — i.e. it read no block-context value. Such a result can
// be kept live by ACCOUNT-granularity watching, which observes both storage and
// balance changes (as kaia_subscribe("callResults") does). For STORAGE-only
// watching, which cannot observe balances, see StorageTrackable.
func (t *BlockswordsCallDependencyTracer) Trackable() bool {
	return len(t.blockContextOps) == 0
}

// StorageTrackable reports whether the call's result is a pure function of
// STORAGE alone: it read no account balance (BalanceAddresses), no account code
// or existence (CodeAddresses), no account key (KeyAddresses), and no block
// context (BlockContextOpcodes). Only such a result can be kept live by watching
// its accessed storage slots via kaia_subscribe("storageChanges"), which observes
// the storage trie only.
//
// It is strictly stronger than Trackable: a result that also depends on a
// balance, code/existence, or account key is Trackable (account-granularity
// watching, e.g. kaia_subscribe("callResults"), observes those) but NOT
// StorageTrackable, because they change outside the storage trie and a
// storage-only watch would serve it stale.
//
// It treats the code of an ordinary contract the call invokes (CALL/STATICCALL/
// DELEGATECALL) as immutable — the callee's behavior is decomposed into the reads
// it performs (storage, balance, explicit EXTCODE*), which are captured. An
// EIP-7702 delegation target IS recorded (CodeAddresses), but a regular callee
// being deployed-into or self-destructed+redeployed is not reflected here; such
// calls should use the account-granularity callResults engine.
func (t *BlockswordsCallDependencyTracer) StorageTrackable() bool {
	return len(t.blockContextOps) == 0 && len(t.balanceAddrs) == 0 &&
		len(t.codeAddrs) == 0 && len(t.keyAddrs) == 0
}
