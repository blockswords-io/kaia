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
	"github.com/kaiachain/kaia/common"
)

// BlockswordsCallDependencyTracer extends AccessListTracer with the extra
// dependency classification the reactive-call ("live kaia_call") engine needs:
//
//   - Balance reads (BALANCE / SELFBALANCE): the result depends on account
//     balances, which change outside the storage trie and so must be watched at
//     account granularity, not via storage slots.
//   - Block-context reads (TIMESTAMP, NUMBER, BASEFEE, ...): a result that reads
//     these is not a pure function of state, so it cannot be kept live by
//     watching state — every block could change it. The engine uses this to
//     refuse or warn instead of silently serving stale results.
//
// Storage slots and touched addresses are accumulated by the embedded
// AccessListTracer exactly as for eth_createAccessList.
type BlockswordsCallDependencyTracer struct {
	*AccessListTracer
	balanceAddrs    map[common.Address]struct{}
	blockContextOps map[OpCode]struct{}
}

// NewBlockswordsCallDependencyTracer creates a dependency tracer. Pass nil
// addressesToExclude to record every touched address.
func NewBlockswordsCallDependencyTracer(addressesToExclude map[common.Address]struct{}) *BlockswordsCallDependencyTracer {
	return &BlockswordsCallDependencyTracer{
		AccessListTracer: NewAccessListTracer(nil, addressesToExclude),
		balanceAddrs:     make(map[common.Address]struct{}),
		blockContextOps:  make(map[OpCode]struct{}),
	}
}

// CaptureState records storage/address access via the embedded AccessListTracer,
// then additionally classifies balance and block-context reads.
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
	case TIMESTAMP, NUMBER, BLOCKHASH, COINBASE, GASLIMIT, DIFFICULTY, BASEFEE, BLOBBASEFEE:
		t.blockContextOps[op] = struct{}{}
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

// Trackable reports whether the call's result is a pure function of the
// tracked state (storage + balances) — i.e. it read no block-context value.
func (t *BlockswordsCallDependencyTracer) Trackable() bool {
	return len(t.blockContextOps) == 0
}
