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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/kaiachain/kaia/common"
	"github.com/stretchr/testify/assert"
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
	bc := tr.BlockContextOpcodes()
	assert.Contains(t, bc, "TIMESTAMP")
	assert.Contains(t, bc, "NUMBER")
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
	assert.Empty(t, tr.BalanceAddresses())
	assert.Empty(t, tr.BlockContextOpcodes())
}
