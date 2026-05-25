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

package api

import (
	"math/big"
	"testing"
	"time"

	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/state"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/event"
	"github.com/kaiachain/kaia/storage/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeHeadBackend implements only SubscribeChainHeadEvent (backed by a real
// event.Feed); every other Backend method is left nil because the storage-watch
// manager never calls them.
type fakeHeadBackend struct {
	Backend
	feed *event.Feed
}

func (f *fakeHeadBackend) SubscribeChainHeadEvent(ch chan<- blockchain.ChainHeadEvent) event.Subscription {
	return f.feed.Subscribe(ch)
}

func newStorageWatchTestManager() (*storageWatchManager, *event.Feed) {
	feed := new(event.Feed)
	mgr := &storageWatchManager{
		backend: &fakeHeadBackend{feed: feed},
		subs:    make(map[uint64]*storageWatchSubscription),
	}
	return mgr, feed
}

// commitWatchedBlock runs a real StateDB commit (exercising the upstream capture
// hook in updateStorageTrie + the publish in Commit) and returns a synthetic
// canonical block carrying the resulting state root.
func commitWatchedBlock(t *testing.T, number uint64, writes func(*state.StateDB)) *types.Block {
	t.Helper()
	st, err := state.New(common.Hash{}, state.NewDatabase(database.NewMemoryDBManager()), nil, nil)
	require.NoError(t, err)
	writes(st)
	root, err := st.Commit(false)
	require.NoError(t, err)
	return types.NewBlockWithHeader(&types.Header{Number: new(big.Int).SetUint64(number), Root: root})
}

// bareBlock is a block carrying no watched changes, used to drive the anchor.
func bareBlock(number uint64) *types.Block {
	return types.NewBlockWithHeader(&types.Header{Number: new(big.Int).SetUint64(number), Root: common.Hash{}})
}

func recvNotification(t *testing.T, sub *storageWatchSubscription) *StorageChangeNotification {
	t.Helper()
	select {
	case n := <-sub.ch:
		return n
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a storage-change notification")
		return nil
	}
}

// anchor drives the first head event and consumes the resulting "ready" frame,
// asserting its anchor block, leaving the subscription ready for change frames.
func anchor(t *testing.T, mgr *storageWatchManager, feed *event.Feed, sub *storageWatchSubscription, anchorNum uint64) {
	t.Helper()
	feed.Send(blockchain.ChainHeadEvent{Block: bareBlock(anchorNum)})
	ready := recvNotification(t, sub)
	require.Equal(t, StorageWatchReady, ready.Type, "first message must be the ready anchor")
	require.EqualValues(t, anchorNum, ready.BlockNumber)
}

// TestStorageWatchManagerEndToEnd drives the full cross-package path: subscribe
// -> real StateDB commit (capture + publish) -> ChainHeadEvent -> manager
// dispatch -> per-subscription filtered, sequence-numbered notification.
func TestStorageWatchManagerEndToEnd(t *testing.T) {
	defer state.SetBlockswordsStorageWatchlist(nil)

	watched := common.HexToAddress("0x1111")
	unwatched := common.HexToAddress("0x2222")
	key := common.HexToHash("0xaa")
	val := common.HexToHash("0x42")

	mgr, feed := newStorageWatchTestManager()
	sub := mgr.subscribe(map[common.Address]map[common.Hash]struct{}{watched: nil}) // all slots
	defer mgr.unsubscribe(sub)

	// First head anchors the subscription at block 100 (changes start after it).
	anchor(t, mgr, feed, sub, 100)

	block := commitWatchedBlock(t, 101, func(st *state.StateDB) {
		st.SetState(watched, key, val)
		st.SetState(unwatched, common.HexToHash("0xbb"), common.HexToHash("0x99")) // must be filtered out
	})
	feed.Send(blockchain.ChainHeadEvent{Block: block})

	n := recvNotification(t, sub)
	assert.Equal(t, StorageWatchChanges, n.Type)
	assert.EqualValues(t, 1, n.Seq, "first change message has seq 1")
	assert.EqualValues(t, 101, n.BlockNumber)
	assert.Equal(t, block.Hash(), n.BlockHash)
	require.Len(t, n.Changes, 1, "only the watched contract's change is delivered")
	assert.Equal(t, watched, n.Changes[0].Address)
	assert.Equal(t, key, n.Changes[0].Key)
	assert.Equal(t, common.Hash{}, n.Changes[0].Previous)
	assert.Equal(t, val, n.Changes[0].Value)

	// A second changed block advances seq monotonically.
	block2 := commitWatchedBlock(t, 102, func(st *state.StateDB) {
		st.SetState(watched, key, common.HexToHash("0x43"))
	})
	feed.Send(blockchain.ChainHeadEvent{Block: block2})

	n2 := recvNotification(t, sub)
	assert.EqualValues(t, 2, n2.Seq, "seq increments per delivered message")
	assert.EqualValues(t, 102, n2.BlockNumber)
}

// TestStorageWatchManagerNoChangeNoNotification verifies a block that does not
// touch any watched contract yields no message.
func TestStorageWatchManagerNoChangeNoNotification(t *testing.T) {
	defer state.SetBlockswordsStorageWatchlist(nil)

	watched := common.HexToAddress("0x3333")
	mgr, feed := newStorageWatchTestManager()
	sub := mgr.subscribe(map[common.Address]map[common.Hash]struct{}{watched: nil})
	defer mgr.unsubscribe(sub)

	anchor(t, mgr, feed, sub, 200)

	block := commitWatchedBlock(t, 201, func(st *state.StateDB) {
		st.SetState(common.HexToAddress("0x4444"), common.HexToHash("0x01"), common.HexToHash("0x02"))
	})
	feed.Send(blockchain.ChainHeadEvent{Block: block})

	select {
	case n := <-sub.ch:
		t.Fatalf("unexpected notification: %+v", n)
	case <-time.After(200 * time.Millisecond):
		// expected: nothing to deliver
	}
}

// TestStorageWatchManagerSlotGranularity verifies a slot-granular subscription
// receives only its listed slot, even when the contract changed other slots.
func TestStorageWatchManagerSlotGranularity(t *testing.T) {
	defer state.SetBlockswordsStorageWatchlist(nil)

	watched := common.HexToAddress("0x5555")
	k1 := common.HexToHash("0xa1")
	k2 := common.HexToHash("0xa2")

	mgr, feed := newStorageWatchTestManager()
	sub := mgr.subscribe(map[common.Address]map[common.Hash]struct{}{
		watched: {k1: struct{}{}},
	})
	defer mgr.unsubscribe(sub)

	anchor(t, mgr, feed, sub, 300)

	block := commitWatchedBlock(t, 301, func(st *state.StateDB) {
		st.SetState(watched, k1, common.HexToHash("0x11"))
		st.SetState(watched, k2, common.HexToHash("0x22")) // unwatched slot
	})
	feed.Send(blockchain.ChainHeadEvent{Block: block})

	n := recvNotification(t, sub)
	assert.Equal(t, StorageWatchChanges, n.Type)
	require.Len(t, n.Changes, 1)
	assert.Equal(t, k1, n.Changes[0].Key)
}

// TestStorageWatchManagerWatchlistLifecycle verifies the global capture gate is
// widened/narrowed/cleared as subscriptions come and go.
func TestStorageWatchManagerWatchlistLifecycle(t *testing.T) {
	defer state.SetBlockswordsStorageWatchlist(nil)

	a := common.HexToAddress("0xA")
	kB := common.HexToHash("0x0B")
	other := common.HexToHash("0x0C")

	mgr, _ := newStorageWatchTestManager()

	// sub1 watches A/slot B only.
	sub1 := mgr.subscribe(map[common.Address]map[common.Hash]struct{}{a: {kB: struct{}{}}})
	assert.True(t, stateWatched(a, kB))
	assert.False(t, stateWatched(a, other), "only slot B is watched")

	// sub2 widens A to all slots.
	sub2 := mgr.subscribe(map[common.Address]map[common.Hash]struct{}{a: nil})
	assert.True(t, stateWatched(a, other), "widened to all slots")

	// Cancelling sub2 narrows A back to slot B.
	mgr.unsubscribe(sub2)
	assert.True(t, stateWatched(a, kB))
	assert.False(t, stateWatched(a, other), "narrowed back to slot B")

	// Cancelling sub1 disables capture entirely.
	mgr.unsubscribe(sub1)
	assert.False(t, stateWatched(a, kB))
}

// stateWatched is a tiny probe that re-derives the gate decision the way the
// capture path does, by committing a single write and checking whether it was
// captured. It keeps the lifecycle test independent of state-package internals.
func stateWatched(addr common.Address, key common.Hash) bool {
	st, err := state.New(common.Hash{}, state.NewDatabase(database.NewMemoryDBManager()), nil, nil)
	if err != nil {
		return false
	}
	st.SetState(addr, key, common.HexToHash("0x01"))
	root, err := st.Commit(false)
	if err != nil {
		return false
	}
	return len(state.LookupBlockswordsStorageChanges(root)) > 0
}
