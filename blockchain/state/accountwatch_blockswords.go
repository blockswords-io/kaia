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

// Blockswords fork-only account-level change watch is kept in this file to
// minimize merge conflicts with upstream Kaia files.
//
// Unlike storagewatch_blockswords.go (which captures (address, slot) storage
// writes), this captures changes at ACCOUNT granularity: it reports an address
// whenever its account changed in a block — balance, nonce, code, or storage
// root. That is exactly what the reactive-call engine needs to watch a call's
// balance dependencies (balances change outside the storage trie) and, in fact,
// every dependency uniformly (a storage write also changes the account's storage
// root). It carries no values: a watcher re-evaluates on the signal.
//
// The only upstream touch points are the blockswordsAccountWatch field on
// StateDB and the capture/publish calls in StateDB.Commit (statedb.go).

import (
	"sync/atomic"

	"github.com/kaiachain/kaia/common"
)

// blockswordsAccountWatchlist is the global set of addresses whose account-level
// changes should be captured. Read on the hot commit path, replaced wholesale
// (copy-on-write) when subscriptions change; a nil pointer disables capture.
var blockswordsAccountWatchlist atomic.Pointer[map[common.Address]struct{}]

// SetBlockswordsAccountWatchlist replaces the global account watch-list. An
// empty list disables capture. The caller transfers ownership of watch and must
// not mutate it afterwards. Safe for concurrent use.
func SetBlockswordsAccountWatchlist(addrs []common.Address) {
	if len(addrs) == 0 {
		blockswordsAccountWatchlist.Store(nil)
		return
	}
	m := make(map[common.Address]struct{}, len(addrs))
	for _, addr := range addrs {
		m[addr] = struct{}{}
	}
	blockswordsAccountWatchlist.Store(&m)
}

func blockswordsAccountWatched(addr common.Address) bool {
	m := blockswordsAccountWatchlist.Load()
	if m == nil {
		return false
	}
	_, ok := (*m)[addr]
	return ok
}

// blockswordsAccountWatchBuffer accumulates the watched addresses that changed
// in one block. Touched only by the goroutine processing its StateDB.
type blockswordsAccountWatchBuffer struct {
	changed map[common.Address]struct{}
}

func newBlockswordsAccountWatchBuffer() *blockswordsAccountWatchBuffer {
	return &blockswordsAccountWatchBuffer{changed: make(map[common.Address]struct{})}
}

func (b *blockswordsAccountWatchBuffer) record(addr common.Address) {
	b.changed[addr] = struct{}{}
}

func (b *blockswordsAccountWatchBuffer) addresses() []common.Address {
	out := make([]common.Address, 0, len(b.changed))
	for addr := range b.changed {
		out = append(out, addr)
	}
	return out
}

// blockswordsCaptureAccountChange is invoked from StateDB.Commit for every
// committed-dirty account. It is a no-op (one atomic load) unless the account is
// on the watch-list.
func (s *StateDB) blockswordsCaptureAccountChange(addr common.Address) {
	if !blockswordsAccountWatched(addr) {
		return
	}
	if s.blockswordsAccountWatch == nil {
		s.blockswordsAccountWatch = newBlockswordsAccountWatchBuffer()
	}
	s.blockswordsAccountWatch.record(addr)
}

// blockswordsPublishAccountChanges is invoked from StateDB.Commit once the new
// state root is known; it publishes the block's watched account changes keyed by
// root, where the RPC layer joins them against the canonical ChainHeadEvent.
func (s *StateDB) blockswordsPublishAccountChanges(root common.Hash) {
	if s.blockswordsAccountWatch == nil {
		return
	}
	blockswordsAccountRing.publish(root, s.blockswordsAccountWatch.addresses())
	s.blockswordsAccountWatch = nil
}

// blockswordsAccountWatchRingSize bounds how many recent root->changed-addresses
// entries are retained. See blockswordsWatchRing (watchring_blockswords.go).
const blockswordsAccountWatchRingSize = 256

var blockswordsAccountRing = newBlockswordsWatchRing[common.Address](blockswordsAccountWatchRingSize)

// LookupBlockswordsAccountChanges returns and removes the watched addresses that
// changed in the block with the given state root, or nil if none.
func LookupBlockswordsAccountChanges(root common.Hash) []common.Address {
	return blockswordsAccountRing.lookup(root)
}
