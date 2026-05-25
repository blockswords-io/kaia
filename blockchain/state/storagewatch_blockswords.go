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

// Blockswords fork-only storage-write watch is kept in this file to minimize
// merge conflicts with upstream Kaia files. It taps the single storage-write
// chokepoint (stateObject.updateStorageTrie) to capture the exact set of
// (address, slot) values a block changes, with no state reads — so it behaves
// identically whether or not snapshots are enabled.
//
// The only upstream touch points are:
//   - the blockswordsStorageWatch field on StateDB (statedb.go),
//   - the blockswordsCaptureStorageWrite call in updateStorageTrie (state_object.go),
//   - the blockswordsPublishStorageWrites call in Commit (statedb.go).
//
// Everything else (the watch-list gate, the per-block capture buffer, and the
// bounded root->delta ring consumed by the RPC layer) lives here.

import (
	"sync/atomic"

	"github.com/kaiachain/kaia/common"
)

// BlockswordsStorageChange is one net storage change committed by a block:
// Previous and Value are the slot's value at the parent block and at this block
// respectively. A zero Value denotes a cleared slot.
type BlockswordsStorageChange struct {
	Address  common.Address
	Key      common.Hash
	Previous common.Hash
	Value    common.Hash
}

// blockswordsStorageWatchlist maps a watched contract to its watched slot-set,
// where a nil slot-set means "every slot of this contract" (contract
// granularity) and a non-nil set means "only these slots" (slot granularity).
// It is read on the hot commit path (once per changed slot) and replaced
// wholesale (copy-on-write) when RPC subscriptions change, so reads are
// lock-free. A nil pointer means the watch is disabled and capture is a single
// atomic load followed by an early return.
var blockswordsStorageWatchlist atomic.Pointer[map[common.Address]map[common.Hash]struct{}]

// SetBlockswordsStorageWatchlist replaces the global watch-list. A nil slot-set
// for an address watches all of its slots; a non-nil set watches only those
// slots. An empty watch-list disables capture entirely. The caller transfers
// ownership of watch and must not mutate it afterwards. Safe for concurrent use.
func SetBlockswordsStorageWatchlist(watch map[common.Address]map[common.Hash]struct{}) {
	if len(watch) == 0 {
		blockswordsStorageWatchlist.Store(nil)
		return
	}
	blockswordsStorageWatchlist.Store(&watch)
}

// blockswordsStorageWatchedSlot reports whether (addr, key) is covered by the
// watch-list: either addr is watched at contract granularity (nil slot-set) or
// the specific (addr, key) slot is listed.
func blockswordsStorageWatchedSlot(addr common.Address, key common.Hash) bool {
	m := blockswordsStorageWatchlist.Load()
	if m == nil {
		return false
	}
	slots, ok := (*m)[addr]
	if !ok {
		return false
	}
	if slots == nil {
		return true
	}
	_, ok = slots[key]
	return ok
}

// blockswordsStorageWatchEntry tracks the first-seen previous value and the
// latest value of a single slot across the (possibly many) intra-block flushes.
type blockswordsStorageWatchEntry struct {
	previous common.Hash
	value    common.Hash
}

// blockswordsStorageWatchBuffer accumulates captured writes for one StateDB
// across a block. It is only ever touched by the single goroutine processing
// that StateDB, so it needs no synchronization.
type blockswordsStorageWatchBuffer struct {
	changes map[common.Address]map[common.Hash]blockswordsStorageWatchEntry
}

func newBlockswordsStorageWatchBuffer() *blockswordsStorageWatchBuffer {
	return &blockswordsStorageWatchBuffer{
		changes: make(map[common.Address]map[common.Hash]blockswordsStorageWatchEntry),
	}
}

// record stores a captured write. The previous value is kept from the first
// time a slot is seen in the block (its parent-block value); the value is
// always the most recent write.
func (b *blockswordsStorageWatchBuffer) record(addr common.Address, key, previous, value common.Hash) {
	slots := b.changes[addr]
	if slots == nil {
		slots = make(map[common.Hash]blockswordsStorageWatchEntry)
		b.changes[addr] = slots
	}
	if entry, ok := slots[key]; ok {
		entry.value = value
		slots[key] = entry
		return
	}
	slots[key] = blockswordsStorageWatchEntry{previous: previous, value: value}
}

// netChanges returns the slots whose value actually differs from the parent
// block, dropping intra-block round-trips that net to no change.
func (b *blockswordsStorageWatchBuffer) netChanges() []BlockswordsStorageChange {
	out := make([]BlockswordsStorageChange, 0, len(b.changes))
	for addr, slots := range b.changes {
		for key, entry := range slots {
			if entry.previous == entry.value {
				continue
			}
			out = append(out, BlockswordsStorageChange{
				Address:  addr,
				Key:      key,
				Previous: entry.previous,
				Value:    entry.value,
			})
		}
	}
	return out
}

// blockswordsCaptureStorageWrite is invoked from stateObject.updateStorageTrie
// for every non-noop storage write. It is a no-op (one atomic load) unless the
// owning contract is on the watch-list.
func (s *StateDB) blockswordsCaptureStorageWrite(addr common.Address, key, previous, value common.Hash) {
	if !blockswordsStorageWatchedSlot(addr, key) {
		return
	}
	if s.blockswordsStorageWatch == nil {
		s.blockswordsStorageWatch = newBlockswordsStorageWatchBuffer()
	}
	s.blockswordsStorageWatch.record(addr, key, previous, value)
}

// blockswordsPublishStorageWrites is invoked from StateDB.Commit once the new
// state root is known. It publishes the block's net watched-storage delta keyed
// by root, where the RPC layer joins it against the canonical ChainHeadEvent.
func (s *StateDB) blockswordsPublishStorageWrites(root common.Hash) {
	if s.blockswordsStorageWatch == nil {
		return
	}
	blockswordsStorageRing.publish(root, s.blockswordsStorageWatch.netChanges())
	s.blockswordsStorageWatch = nil
}

// blockswordsStorageWatchRingSize bounds how many recent root->delta entries are
// retained. See blockswordsWatchRing (watchring_blockswords.go).
const blockswordsStorageWatchRingSize = 256

var blockswordsStorageRing = newBlockswordsWatchRing[BlockswordsStorageChange](blockswordsStorageWatchRingSize)

// LookupBlockswordsStorageChanges returns and removes the net storage delta
// published for the given state root, or nil if none was recorded.
func LookupBlockswordsStorageChanges(root common.Hash) []BlockswordsStorageChange {
	return blockswordsStorageRing.lookup(root)
}
