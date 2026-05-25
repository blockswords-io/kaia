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
	"testing"

	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/storage/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStateDB(t *testing.T) *StateDB {
	t.Helper()
	st, err := New(common.Hash{}, NewDatabase(database.NewMemoryDBManager()), nil, nil)
	require.NoError(t, err)
	return st
}

func TestBlockswordsStorageWatchGate(t *testing.T) {
	addrAll := common.HexToAddress("0xabc")  // watched at contract granularity
	addrSlot := common.HexToAddress("0xdef") // watched only at slot k1
	k1 := common.HexToHash("0x01")
	k2 := common.HexToHash("0x02")
	defer SetBlockswordsStorageWatchlist(nil)

	SetBlockswordsStorageWatchlist(nil)
	assert.False(t, blockswordsStorageWatchedSlot(addrAll, k1))

	SetBlockswordsStorageWatchlist(map[common.Address]map[common.Hash]struct{}{
		addrAll:  nil,
		addrSlot: {k1: struct{}{}},
	})
	assert.True(t, blockswordsStorageWatchedSlot(addrAll, k1))
	assert.True(t, blockswordsStorageWatchedSlot(addrAll, k2), "nil slot-set watches all slots")
	assert.True(t, blockswordsStorageWatchedSlot(addrSlot, k1))
	assert.False(t, blockswordsStorageWatchedSlot(addrSlot, k2), "unwatched slot of a slot-granular address")
	assert.False(t, blockswordsStorageWatchedSlot(common.HexToAddress("0x999"), k1))

	SetBlockswordsStorageWatchlist(nil) // empty disables
	assert.False(t, blockswordsStorageWatchedSlot(addrAll, k1))
}

func TestBlockswordsStorageWatchBufferNetDelta(t *testing.T) {
	addr := common.HexToAddress("0xabc")
	key := common.HexToHash("0x01")

	b := newBlockswordsStorageWatchBuffer()
	// 0 -> A (flush 1), A -> B (flush 2): net is 0 -> B, reported with parent value 0.
	b.record(addr, key, common.Hash{}, common.HexToHash("0x0A"))
	b.record(addr, key, common.HexToHash("0x0A"), common.HexToHash("0x0B"))

	changes := b.netChanges()
	require.Len(t, changes, 1)
	assert.Equal(t, addr, changes[0].Address)
	assert.Equal(t, key, changes[0].Key)
	assert.Equal(t, common.Hash{}, changes[0].Previous, "previous must be the parent-block value")
	assert.Equal(t, common.HexToHash("0x0B"), changes[0].Value, "value must be the latest write")

	// A round-trip back to the parent value nets to nothing.
	rt := common.HexToHash("0x10")
	b2 := newBlockswordsStorageWatchBuffer()
	b2.record(addr, rt, common.Hash{}, common.HexToHash("0xFF"))
	b2.record(addr, rt, common.HexToHash("0xFF"), common.Hash{})
	assert.Empty(t, b2.netChanges(), "intra-block round-trip must not be reported")
}

func TestBlockswordsWatchRingEviction(t *testing.T) {
	const capacity = 256
	ring := newBlockswordsWatchRing[BlockswordsStorageChange](capacity)
	change := []BlockswordsStorageChange{{Key: common.HexToHash("0x1")}}

	roots := make([]common.Hash, capacity+10)
	for i := range roots {
		roots[i] = common.BytesToHash([]byte{byte(i >> 8), byte(i)})
		ring.publish(roots[i], change)
	}
	// Oldest 10 evicted; newest survive and are consumed on lookup.
	assert.Nil(t, ring.lookup(roots[0]), "evicted root should be gone")
	last := roots[len(roots)-1]
	assert.NotNil(t, ring.lookup(last))
	assert.Nil(t, ring.lookup(last), "lookup must consume the entry")
	assert.Nil(t, ring.lookup(common.HexToHash("0xfeedface")), "absent root returns nil")
	ring.publish(common.HexToHash("0xc0ffee"), nil)
	assert.Nil(t, ring.lookup(common.HexToHash("0xc0ffee")), "empty change set is not stored")
}

// TestBlockswordsStorageWatchEndToEnd exercises the real capture path through
// updateStorageTrie + Commit: only watched contracts' net changes are published,
// keyed by the resulting state root.
func TestBlockswordsStorageWatchEndToEnd(t *testing.T) {
	watched := common.HexToAddress("0x1111")
	unwatched := common.HexToAddress("0x2222")
	defer SetBlockswordsStorageWatchlist(nil)
	SetBlockswordsStorageWatchlist(map[common.Address]map[common.Hash]struct{}{watched: nil})

	st := newTestStateDB(t)
	keyHit := common.HexToHash("0xaa")
	valHit := common.HexToHash("0x42")
	keyMiss := common.HexToHash("0xbb")

	st.SetState(watched, keyHit, valHit)
	st.SetState(unwatched, keyMiss, common.HexToHash("0x99")) // not on the watch-list

	root, err := st.Commit(false)
	require.NoError(t, err)

	changes := LookupBlockswordsStorageChanges(root)
	require.Len(t, changes, 1, "only the watched contract's change should be published")
	assert.Equal(t, watched, changes[0].Address)
	assert.Equal(t, keyHit, changes[0].Key)
	assert.Equal(t, common.Hash{}, changes[0].Previous)
	assert.Equal(t, valHit, changes[0].Value)

	assert.Nil(t, LookupBlockswordsStorageChanges(root), "delta is consumed once")
}

// TestBlockswordsStorageWatchEndToEndNetNoop verifies that a slot written and
// then reverted to its parent value across intra-block flushes is not reported.
func TestBlockswordsStorageWatchEndToEndNetNoop(t *testing.T) {
	watched := common.HexToAddress("0x3333")
	defer SetBlockswordsStorageWatchlist(nil)
	SetBlockswordsStorageWatchlist(map[common.Address]map[common.Hash]struct{}{watched: nil})

	st := newTestStateDB(t)
	key := common.HexToHash("0xcc")

	st.SetState(watched, key, common.HexToHash("0xAA"))
	st.IntermediateRoot(false) // flush #1 captures (key, prev=0, val=0xAA)
	st.SetState(watched, key, common.Hash{})

	root, err := st.Commit(false) // flush #2 captures (key, prev=0xAA, val=0)
	require.NoError(t, err)

	assert.Nil(t, LookupBlockswordsStorageChanges(root), "net-zero change must not be published")
}

// TestBlockswordsStorageWatchEndToEndSlotGranularity verifies that, for an
// address watched at slot granularity, writes to unwatched slots are skipped at
// the capture gate (never buffered), not merely filtered downstream.
func TestBlockswordsStorageWatchEndToEndSlotGranularity(t *testing.T) {
	watched := common.HexToAddress("0x5555")
	k1 := common.HexToHash("0xa1")
	k2 := common.HexToHash("0xa2")
	defer SetBlockswordsStorageWatchlist(nil)
	SetBlockswordsStorageWatchlist(map[common.Address]map[common.Hash]struct{}{
		watched: {k1: struct{}{}},
	})

	st := newTestStateDB(t)
	st.SetState(watched, k1, common.HexToHash("0x11"))
	st.SetState(watched, k2, common.HexToHash("0x22")) // unwatched slot

	root, err := st.Commit(false)
	require.NoError(t, err)

	changes := LookupBlockswordsStorageChanges(root)
	require.Len(t, changes, 1, "only the watched slot should be captured")
	assert.Equal(t, k1, changes[0].Key)
	assert.Equal(t, common.HexToHash("0x11"), changes[0].Value)
}

// TestBlockswordsStorageWatchDisabledNoCapture verifies zero capture (and no
// buffer allocation) when the watch-list is empty.
func TestBlockswordsStorageWatchDisabledNoCapture(t *testing.T) {
	SetBlockswordsStorageWatchlist(nil)
	st := newTestStateDB(t)
	st.SetState(common.HexToAddress("0x4444"), common.HexToHash("0x01"), common.HexToHash("0x02"))
	root, err := st.Commit(false)
	require.NoError(t, err)
	assert.Nil(t, st.blockswordsStorageWatch, "buffer must not be allocated when disabled")
	assert.Nil(t, LookupBlockswordsStorageChanges(root))
}
