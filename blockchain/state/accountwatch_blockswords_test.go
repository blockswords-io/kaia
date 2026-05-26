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

package state

import (
	"math/big"
	"testing"

	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/params"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockswordsAccountWatchBalanceChange(t *testing.T) {
	watched := common.HexToAddress("0x1111")
	unwatched := common.HexToAddress("0x2222")
	defer SetBlockswordsAccountWatchlist(nil)
	SetBlockswordsAccountWatchlist([]common.Address{watched})

	st := newTestStateDB(t)
	st.AddBalance(watched, big.NewInt(100))  // balance change dirties the account
	st.AddBalance(unwatched, big.NewInt(50)) // not on the watch-list

	root, err := st.Commit(false)
	require.NoError(t, err)

	changed := LookupBlockswordsAccountChanges(root)
	require.Len(t, changed, 1, "only the watched account's change is published")
	assert.Equal(t, watched, changed[0])
	assert.Nil(t, LookupBlockswordsAccountChanges(root), "delta is consumed once")
}

// A storage write also changes the account (its storage root), so account-level
// capture fires for it too — the engine watches every dep uniformly this way.
func TestBlockswordsAccountWatchStorageImpliesAccount(t *testing.T) {
	watched := common.HexToAddress("0x3333")
	defer SetBlockswordsAccountWatchlist(nil)
	SetBlockswordsAccountWatchlist([]common.Address{watched})

	st := newTestStateDB(t)
	st.SetState(watched, common.HexToHash("0x01"), common.HexToHash("0x02"))

	root, err := st.Commit(false)
	require.NoError(t, err)

	changed := LookupBlockswordsAccountChanges(root)
	require.Len(t, changed, 1)
	assert.Equal(t, watched, changed[0])
}

// A code change (a first deployment, or an EIP-7702 (re)delegation) dirties the
// account, so account-level capture fires for it too. This is why a callResults
// dependency whose code/existence the call reads (EXTCODESIZE/EXTCODEHASH/
// EXTCODECOPY, or a CALL target) is re-evaluated when that code changes —
// account-granularity watching covers code without a separate code watch-list,
// even though deployed runtime code is otherwise immutable.
func TestBlockswordsAccountWatchCodeChange(t *testing.T) {
	watched := common.HexToAddress("0x5555")
	defer SetBlockswordsAccountWatchlist(nil)
	SetBlockswordsAccountWatchlist([]common.Address{watched})

	st := newTestStateDB(t)
	st.CreateSmartContractAccount(watched, params.CodeFormatEVM, params.Rules{})
	require.NoError(t, st.SetCode(watched, []byte{0x60, 0x00})) // PUSH1 0

	root, err := st.Commit(false)
	require.NoError(t, err)

	changed := LookupBlockswordsAccountChanges(root)
	require.Len(t, changed, 1)
	assert.Equal(t, watched, changed[0])
}

func TestBlockswordsAccountWatchDisabledNoCapture(t *testing.T) {
	SetBlockswordsAccountWatchlist(nil)
	st := newTestStateDB(t)
	st.AddBalance(common.HexToAddress("0x4444"), big.NewInt(1))
	root, err := st.Commit(false)
	require.NoError(t, err)
	assert.Nil(t, st.blockswordsAccountWatch, "buffer must not be allocated when disabled")
	assert.Nil(t, LookupBlockswordsAccountChanges(root))
}
