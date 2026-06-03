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

func newStorageWatchTestManager(t *testing.T) (*storageWatchManager, *event.Feed) {
	feed := new(event.Feed)
	mgr := &storageWatchManager{
		backend: &fakeHeadBackend{feed: feed},
		subs:    make(map[uint64]*storageWatchSubscription),
	}
	// The manager no longer stops its head loop on last-unsubscribe (it is a
	// process-lifetime singleton in production), so stop it explicitly when the
	// test ends to avoid leaking the head-loop goroutine.
	t.Cleanup(mgr.close)
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

	mgr, feed := newStorageWatchTestManager(t)
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
	mgr, feed := newStorageWatchTestManager(t)
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

	mgr, feed := newStorageWatchTestManager(t)
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

	mgr, _ := newStorageWatchTestManager(t)

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

// TestStorageWatchManagerHeadJumpReAnchors verifies that a batch-insert head jump
// (a ChainHeadEvent that skips one or more blocks) re-anchors an already-anchored
// subscription with a fresh "ready" frame rather than silently delivering only the
// head block's delta — so the client re-snapshots and no skipped-block change is
// lost without a detectable signal.
func TestStorageWatchManagerHeadJumpReAnchors(t *testing.T) {
	defer state.SetBlockswordsStorageWatchlist(nil)
	watched := common.HexToAddress("0x6666")
	key := common.HexToHash("0xc1")

	mgr, feed := newStorageWatchTestManager(t)
	sub := mgr.subscribe(map[common.Address]map[common.Hash]struct{}{watched: nil})
	defer mgr.unsubscribe(sub)

	anchor(t, mgr, feed, sub, 100)

	// Contiguous block 101 delivers a normal change (seq 1).
	b101 := commitWatchedBlock(t, 101, func(st *state.StateDB) { st.SetState(watched, key, common.HexToHash("0x01")) })
	feed.Send(blockchain.ChainHeadEvent{Block: b101})
	n := recvNotification(t, sub)
	require.Equal(t, StorageWatchChanges, n.Type)
	require.EqualValues(t, 1, n.Seq)

	// A batch head jump to 103 skips block 102: the subscription must RE-ANCHOR
	// (a fresh "ready" naming 103), not silently deliver only 103's delta.
	b103 := commitWatchedBlock(t, 103, func(st *state.StateDB) { st.SetState(watched, key, common.HexToHash("0x02")) })
	feed.Send(blockchain.ChainHeadEvent{Block: b103})
	rj := recvNotification(t, sub)
	require.Equal(t, StorageWatchReady, rj.Type, "a head jump must re-anchor with a fresh ready frame")
	require.EqualValues(t, 103, rj.BlockNumber)

	// After the re-anchor the delta stream resumes with seq reset to 1.
	b104 := commitWatchedBlock(t, 104, func(st *state.StateDB) { st.SetState(watched, key, common.HexToHash("0x03")) })
	feed.Send(blockchain.ChainHeadEvent{Block: b104})
	n2 := recvNotification(t, sub)
	require.Equal(t, StorageWatchChanges, n2.Type)
	require.EqualValues(t, 1, n2.Seq, "seq restarts at 1 after a re-anchor")
	require.EqualValues(t, 104, n2.BlockNumber)
}

// TestStorageWatchManagerReAnchorRetriesOnFullBuffer verifies that when a head-jump
// re-anchor "ready" cannot be delivered (consumer buffer full), the subscription
// stays un-anchored and the ready is re-attempted on the next dispatch — so the
// re-snapshot signal is never silently lost. Driven via direct dispatch.
func TestStorageWatchManagerReAnchorRetriesOnFullBuffer(t *testing.T) {
	defer state.SetBlockswordsStorageWatchlist(nil)
	watched := common.HexToAddress("0xbabe")
	key := common.HexToHash("0xf1")

	mgr, _ := newStorageWatchTestManager(t)
	sub := &storageWatchSubscription{
		id:      1,
		filters: map[common.Address]map[common.Hash]struct{}{watched: nil},
		ch:      make(chan *StorageChangeNotification, storageWatchBacklog),
	}
	mgr.subs[sub.id] = sub
	mgr.rebuildSubsSnapshotLocked() // dispatch reads the lock-free snapshot
	state.SetBlockswordsStorageWatchlist(map[common.Address]map[common.Hash]struct{}{watched: nil})

	mgr.dispatch(bareBlock(1000)) // initial anchor (ready in buffer)
	require.True(t, sub.anchored)

	// Fill the buffer completely with change frames (none drained).
	for i := 0; i < storageWatchBacklog; i++ {
		b := commitWatchedBlock(t, uint64(1001+i), func(st *state.StateDB) {
			st.SetState(watched, key, common.BytesToHash([]byte{byte(i%250 + 1)}))
		})
		mgr.dispatch(b)
	}
	require.Len(t, sub.ch, storageWatchBacklog, "buffer is full")

	// A head jump now: the re-anchor ready cannot be enqueued (buffer full), so the
	// sub must NOT flip to anchored — it stays un-anchored, pending retry.
	jumpBlk := commitWatchedBlock(t, 2000, func(st *state.StateDB) { st.SetState(watched, key, common.HexToHash("0x09")) })
	mgr.dispatch(jumpBlk)
	require.False(t, sub.anchored, "re-anchor must not flip anchored when the ready was dropped")

	// Drain one slot; the next dispatch re-attempts the ready and it lands.
	<-sub.ch
	retryBlk := commitWatchedBlock(t, 2001, func(st *state.StateDB) { st.SetState(watched, key, common.HexToHash("0x0a")) })
	mgr.dispatch(retryBlk)
	require.True(t, sub.anchored, "re-anchor succeeds once buffer has room")

	// The delivered tail must contain a fresh ready frame (the re-snapshot signal),
	// proving it was not silently lost.
	var sawReadyAfterChanges bool
	var lastType string
	for drained := false; !drained; {
		select {
		case n := <-sub.ch:
			if n.Type == StorageWatchReady && lastType == StorageWatchChanges {
				sawReadyAfterChanges = true
			}
			lastType = n.Type
		default:
			drained = true
		}
	}
	require.True(t, sawReadyAfterChanges, "a re-anchor ready must eventually reach the client after the change frames")
}

// TestStorageWatchManagerDropExposesSeqGap verifies the documented resync trigger:
// when the per-subscription buffer fills, sends are dropped but seq still advances
// per change block, so the delivered tail is truncated and a consumer detects the
// gap. Driven via direct dispatch (no head-loop goroutine) for determinism.
func TestStorageWatchManagerDropExposesSeqGap(t *testing.T) {
	defer state.SetBlockswordsStorageWatchlist(nil)
	watched := common.HexToAddress("0x7777")
	key := common.HexToHash("0xd1")

	mgr, _ := newStorageWatchTestManager(t)
	sub := &storageWatchSubscription{
		id:      1,
		filters: map[common.Address]map[common.Hash]struct{}{watched: nil},
		ch:      make(chan *StorageChangeNotification, storageWatchBacklog),
	}
	mgr.subs[sub.id] = sub
	mgr.rebuildSubsSnapshotLocked() // dispatch reads the lock-free snapshot
	state.SetBlockswordsStorageWatchlist(map[common.Address]map[common.Hash]struct{}{watched: nil})

	mgr.dispatch(bareBlock(1000)) // anchor (consumes one buffer slot)

	total := storageWatchBacklog + 8
	for i := 0; i < total; i++ {
		b := commitWatchedBlock(t, uint64(1001+i), func(st *state.StateDB) {
			st.SetState(watched, key, common.BytesToHash([]byte{byte(i%250 + 1)}))
		})
		mgr.dispatch(b)
	}
	require.EqualValues(t, total, sub.seq, "seq advances on every change block, including dropped ones")

	var seqs []uint64
	for drained := false; !drained; {
		select {
		case n := <-sub.ch:
			if n.Type == StorageWatchChanges {
				seqs = append(seqs, uint64(n.Seq))
			}
		default:
			drained = true
		}
	}
	require.NotEmpty(t, seqs)
	require.EqualValues(t, 1, seqs[0])
	for i := 1; i < len(seqs); i++ {
		require.Greater(t, seqs[i], seqs[i-1], "delivered seqs are strictly increasing")
	}
	require.Less(t, seqs[len(seqs)-1], uint64(total), "dropped tail leaves a detectable seq gap")
}

// TestStorageWatchManagerMultiSubscriberFanout verifies two subscriptions with
// different filters each receive only their matching changes with independent seq.
func TestStorageWatchManagerMultiSubscriberFanout(t *testing.T) {
	defer state.SetBlockswordsStorageWatchlist(nil)
	a := common.HexToAddress("0x8888")
	b := common.HexToAddress("0x9999")
	slot := common.HexToHash("0xe1")
	other := common.HexToHash("0xe2")

	mgr, feed := newStorageWatchTestManager(t)
	sub1 := mgr.subscribe(map[common.Address]map[common.Hash]struct{}{a: nil}) // all of A
	defer mgr.unsubscribe(sub1)
	sub2 := mgr.subscribe(map[common.Address]map[common.Hash]struct{}{a: {slot: struct{}{}}, b: nil}) // A/slot + all of B
	defer mgr.unsubscribe(sub2)

	feed.Send(blockchain.ChainHeadEvent{Block: bareBlock(500)})
	require.Equal(t, StorageWatchReady, recvNotification(t, sub1).Type)
	require.Equal(t, StorageWatchReady, recvNotification(t, sub2).Type)

	block := commitWatchedBlock(t, 501, func(st *state.StateDB) {
		st.SetState(a, slot, common.HexToHash("0x11"))
		st.SetState(a, other, common.HexToHash("0x22"))
		st.SetState(b, common.HexToHash("0xf0"), common.HexToHash("0x33"))
	})
	feed.Send(blockchain.ChainHeadEvent{Block: block})

	n1 := recvNotification(t, sub1)
	require.Equal(t, StorageWatchChanges, n1.Type)
	require.EqualValues(t, 1, n1.Seq)
	require.Len(t, n1.Changes, 2, "sub1 sees both of A's changed slots")

	n2 := recvNotification(t, sub2)
	require.Equal(t, StorageWatchChanges, n2.Type)
	require.EqualValues(t, 1, n2.Seq, "sub2 has its own independent seq")
	require.Len(t, n2.Changes, 2, "sub2 sees A/slot and B, not A/other")
	for _, c := range n2.Changes {
		require.False(t, c.Address == a && c.Key == other, "sub2 must not see A's unwatched slot")
	}
}

// TestParseStorageWatchFilters covers the filter merge rules: all-slots-wins,
// order independence, slot-union, and dedup.
func TestParseStorageWatchFilters(t *testing.T) {
	a := common.HexToAddress("0x1")
	b := common.HexToAddress("0x2")
	k1 := common.HexToHash("0x11")
	k2 := common.HexToHash("0x12")

	got := parseStorageWatchFilters([]StorageWatchFilter{{Address: a}})
	slots, ok := got[a]
	require.True(t, ok)
	require.Nil(t, slots, "empty slot list means all slots")

	got = parseStorageWatchFilters([]StorageWatchFilter{{Address: a, Slots: []common.Hash{k1}}, {Address: a}})
	slots, ok = got[a]
	require.True(t, ok)
	require.Nil(t, slots, "empty after specific widens to all slots")

	got = parseStorageWatchFilters([]StorageWatchFilter{{Address: a}, {Address: a, Slots: []common.Hash{k1}}})
	slots, ok = got[a]
	require.True(t, ok)
	require.Nil(t, slots, "specific after all stays all slots")

	got = parseStorageWatchFilters([]StorageWatchFilter{{Address: a, Slots: []common.Hash{k1, k1}}, {Address: a, Slots: []common.Hash{k2}}})
	slots = got[a]
	require.Len(t, slots, 2, "slots union with dedup")
	_, has1 := slots[k1]
	_, has2 := slots[k2]
	require.True(t, has1 && has2)

	got = parseStorageWatchFilters([]StorageWatchFilter{{Address: a, Slots: []common.Hash{k1}}, {Address: b}})
	require.Len(t, got, 2)
	require.NotNil(t, got[a])
	require.Nil(t, got[b], "distinct addresses are independent")
}
