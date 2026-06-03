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

// Blockswords fork-only storage-change subscription is kept in this file to
// minimize merge conflicts with upstream Kaia files.
//
// kaia_subscribe("storageChanges", filters) streams the net storage changes a
// newly inserted (canonical) block makes to the watched contracts. Capture
// happens at the state-write chokepoint with no state reads, so it works
// identically with or without snapshots; this layer only joins the captured
// per-root delta against the canonical ChainHeadEvent and fans it out, filtered
// per subscription, to clients.

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/state"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/networks/rpc"
	"github.com/rcrowley/go-metrics"
)

var (
	// storageWatchSubsGauge tracks the number of active storageChanges
	// subscriptions — the primary load signal for capacity/scale-out decisions.
	storageWatchSubsGauge = metrics.NewRegisteredGauge("kaia/storagewatch/subscriptions", nil)
	// storageWatchDropped counts change notifications dropped because a consumer's
	// buffer was full. A nonzero rate means consumers are falling behind (each drop
	// is a seq gap the client must resync) — a node-overload / scale-out signal.
	storageWatchDropped = metrics.NewRegisteredCounter("kaia/storagewatch/dropped", nil)
)

// StorageWatchFilter selects which storage changes a subscription receives.
// An empty Slots list matches every slot of Address (contract granularity);
// a non-empty list matches only the listed slots (slot granularity).
type StorageWatchFilter struct {
	Address common.Address `json:"address"`
	Slots   []common.Hash  `json:"slots,omitempty"`
}

// StorageChangeEntry is one slot whose value changed in a block.
type StorageChangeEntry struct {
	Address  common.Address `json:"address"`
	Key      common.Hash    `json:"key"`
	Previous common.Hash    `json:"previous"`
	Value    common.Hash    `json:"value"`
}

const (
	// StorageWatchReady is the type of the first message every storageChanges
	// subscription emits. It names the anchor block: the consumer must take its
	// cold-start snapshot pinned at this exact block (e.g. kaia_call /
	// kaia_getStoragesAt with this block number, never "latest"). Every
	// subsequent message covers a strictly later block, with no gap and no
	// overlap, so the snapshot plus the live stream form one consistent view.
	StorageWatchReady = "ready"
	// StorageWatchChanges is the type of a normal change message.
	StorageWatchChanges = "changes"
)

// StorageChangeNotification is one subscription message.
//
// Type is "ready" (the initial anchor frame, carrying only the anchor block) or
// "changes". Seq is a per-subscription monotonic counter starting at 1, set on
// "changes" messages only: because messages are only emitted for blocks that
// touch the subscription's filters, block numbers are naturally non-contiguous,
// so Seq is what lets a consumer detect dropped messages (a gap in Seq). On a
// gap, the consumer resyncs the affected range (e.g. via
// debug_diffContractStorageHash from its last seen block) and keeps consuming.
type StorageChangeNotification struct {
	Type        string               `json:"type"`
	Seq         hexutil.Uint64       `json:"seq,omitempty"`
	BlockNumber hexutil.Uint64       `json:"blockNumber"`
	BlockHash   common.Hash          `json:"blockHash"`
	Changes     []StorageChangeEntry `json:"changes,omitempty"`
}

// storageWatchBacklog bounds the per-subscription buffer. A consumer that falls
// this far behind is dropped (its subscription ends) so it resyncs from a fresh
// snapshot rather than silently missing changes.
const storageWatchBacklog = 512

// storageWatchHeadBacklog bounds the shared chain-head channel.
const storageWatchHeadBacklog = 128

// StorageChanges streams the net storage changes that newly inserted canonical
// blocks make to the contracts/slots selected by filters. It is exposed over
// WebSocket/IPC as kaia_subscribe("storageChanges", filters).
//
// Consistent cold-start protocol (no gap, no lost notifications):
//
//  1. Subscribe. The first message is always {type:"ready", blockNumber:H}.
//  2. Take the cold-start snapshot pinned at block H — pass H as the block
//     parameter to kaia_call / kaia_getStoragesAt / kaia_callWithAccessedStorage,
//     never "latest". H is the anchor: the snapshot reflects state through H.
//  3. Apply every subsequent {type:"changes", blockNumber:>H} message in order.
//     The stream covers all blocks strictly after H, so nothing between the
//     snapshot and the live stream is lost; re-fetch the changed slots (pinned
//     at the message's blockNumber) and upsert idempotently.
//
// Subscribing before snapshotting is what closes the gap; the server picks H as
// the first block it fully observes after the watch is armed, so block H's own
// changes are delivered via the snapshot rather than the stream. On a Seq gap or
// disconnect, resync from the last applied block to a fresh anchor (e.g. via
// debug_diffContractStorageHash) and resubscribe.
//
// NOTE (blockswords fork-only): the underlying capture does not observe
// SELFDESTRUCT-driven storage wipes; such an event will not be reported.
func (s *KaiaBlockChainAPI) StorageChanges(ctx context.Context, filters []StorageWatchFilter) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	if len(filters) == 0 {
		return nil, errors.New("storageChanges: at least one filter is required")
	}
	parsed := parseStorageWatchFilters(filters)

	mgr := storageWatchManagerFor(s.b)
	sub := mgr.subscribe(parsed)
	rpcSub := notifier.CreateSubscription()

	go func() {
		for {
			select {
			case notification := <-sub.ch:
				// A failed notification write means the client connection is broken or
				// has not read within the write-deadline window (default 10s). Tear down
				// immediately so the subscription's watch-list entries (and, when it is
				// the last subscription, the shared capture loop) are released promptly —
				// without waiting for the read side to error, which for a hung (as opposed
				// to cleanly disconnected) client may never happen, since the WebSocket
				// read deadline is disabled by default. The client can resubscribe.
				if err := notifier.Notify(rpcSub.ID, notification); err != nil {
					mgr.unsubscribe(sub)
					return
				}
			case <-rpcSub.Err():
				mgr.unsubscribe(sub)
				return
			case <-notifier.Closed():
				mgr.unsubscribe(sub)
				return
			}
		}
	}()

	return rpcSub, nil
}

// parseStorageWatchFilters merges the request into addr -> slot-set, where a nil
// slot-set means "all slots of this address".
func parseStorageWatchFilters(filters []StorageWatchFilter) map[common.Address]map[common.Hash]struct{} {
	out := make(map[common.Address]map[common.Hash]struct{}, len(filters))
	for _, f := range filters {
		if len(f.Slots) == 0 {
			out[f.Address] = nil // all slots; overrides any prior slot-set
			continue
		}
		slots, present := out[f.Address]
		if present && slots == nil {
			continue // already watching all slots
		}
		if slots == nil {
			slots = make(map[common.Hash]struct{})
			out[f.Address] = slots
		}
		for _, slot := range f.Slots {
			slots[slot] = struct{}{}
		}
	}
	return out
}

// storageWatchSubscription is one client subscription. seq and anchored are
// owned solely by the manager's single dispatch goroutine and need no
// synchronization.
type storageWatchSubscription struct {
	id       uint64
	filters  map[common.Address]map[common.Hash]struct{} // addr -> slot-set; nil set = all slots
	ch       chan *StorageChangeNotification
	seq      uint64
	anchored bool
}

func (sub *storageWatchSubscription) match(c state.BlockswordsStorageChange) bool {
	slots, ok := sub.filters[c.Address]
	if !ok {
		return false
	}
	if slots == nil {
		return true
	}
	_, ok = slots[c.Key]
	return ok
}

// emit performs a non-blocking send and reports whether it was delivered. A full
// buffer means the consumer fell behind; for a "changes" frame the message is
// dropped (the already-advanced seq exposes the gap) and the subscription stays
// alive so the consumer can resync rather than being force-closed. We never block
// here: blocking would stall delivery to other subscribers and back-pressure the
// chain-head feed. The caller uses the return value for the "ready" frame, which
// must not be lost (see dispatch): on a drop the (re-)anchor is retried next block.
func (sub *storageWatchSubscription) emit(n *StorageChangeNotification) bool {
	select {
	case sub.ch <- n:
		return true
	default:
		storageWatchDropped.Inc(1)
		return false
	}
}

// storageWatchManager owns a single ChainHeadEvent subscription and fans the
// per-block captured delta out to all active subscriptions. It is a process
// singleton: there is exactly one KaiaBlockChainAPI backend per node.
type storageWatchManager struct {
	backend Backend

	mu      sync.Mutex
	subs    map[uint64]*storageWatchSubscription
	nextID  uint64
	running bool
	quit    chan struct{}

	// subsSnapshot is a lock-free copy of the subscription set, read by dispatch on
	// the chain-head loop, so dispatch never takes m.mu. ChainHeadEvent is delivered
	// synchronously on the block-import path (PostChainEvents), so a head loop that
	// blocked on m.mu (e.g. behind a refreshWatchlistLocked rebuild during a
	// subscribe burst) would back-pressure the feed and stall block import. Rebuilt
	// under m.mu on every subs change.
	subsSnapshot atomic.Pointer[[]*storageWatchSubscription]

	// lastDispatched is the number of the most recently dispatched head block. It
	// is touched only by the single dispatch goroutine, so it needs no
	// synchronization. Used to detect a multi-block head jump (batch insert).
	lastDispatched uint64
}

// rebuildSubsSnapshotLocked refreshes the lock-free subscription snapshot read by
// dispatch. Must hold m.mu.
func (m *storageWatchManager) rebuildSubsSnapshotLocked() {
	snap := make([]*storageWatchSubscription, 0, len(m.subs))
	for _, sub := range m.subs {
		snap = append(snap, sub)
	}
	m.subsSnapshot.Store(&snap)
}

var (
	storageWatchManagerOnce sync.Once
	storageWatchManagerInst *storageWatchManager
)

func storageWatchManagerFor(b Backend) *storageWatchManager {
	storageWatchManagerOnce.Do(func() {
		storageWatchManagerInst = &storageWatchManager{
			backend: b,
			subs:    make(map[uint64]*storageWatchSubscription),
		}
	})
	return storageWatchManagerInst
}

func (m *storageWatchManager) subscribe(filters map[common.Address]map[common.Hash]struct{}) *storageWatchSubscription {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextID++
	sub := &storageWatchSubscription{
		id:      m.nextID,
		filters: filters,
		ch:      make(chan *StorageChangeNotification, storageWatchBacklog),
	}
	m.subs[sub.id] = sub
	m.rebuildSubsSnapshotLocked()
	storageWatchSubsGauge.Update(int64(len(m.subs)))
	m.refreshWatchlistLocked()
	if !m.running {
		m.startLocked()
	}
	return sub
}

func (m *storageWatchManager) unsubscribe(sub *storageWatchSubscription) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.subs[sub.id]; !ok {
		return
	}
	delete(m.subs, sub.id)
	m.rebuildSubsSnapshotLocked()
	storageWatchSubsGauge.Update(int64(len(m.subs)))
	m.refreshWatchlistLocked()
	// The head loop is deliberately NOT stopped on last-unsubscribe. Stopping and
	// later restarting it would let the dying goroutine and a fresh one briefly
	// contend for the same delete-on-read per-block delta — the exact restart race
	// the reactive manager documents avoiding (see reactivecall startLocked). With
	// no subscriptions the watch-list is empty, so capture is a no-op and the loop
	// idles cheaply (LookupBlockswordsStorageChanges returns nil). close() stops it
	// for test cleanup only.
}

// close stops the chain-head loop. It is intended for test cleanup; production
// uses the process-lifetime singleton and never calls it.
func (m *storageWatchManager) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		m.running = false
		close(m.quit)
	}
}

// refreshWatchlistLocked recomputes the union of every subscription's filters
// (addr -> slot-set, nil set = all slots) and pushes it to the state-layer
// capture gate, so writes to slots no subscription cares about are skipped at
// capture time rather than buffered and filtered later. Must hold m.mu.
//
// Cost: O(S·F) in subscription count S and per-sub filter count F, on subscribe/
// unsubscribe only. With subscriptions uncapped this can spike during a large
// simultaneous subscribe burst, but it is bounded by CPU and m.mu and can NOT
// stall block import — dispatch reads subscriptions via the lock-free subsSnapshot
// and never waits on m.mu. This matches the fork's scale-out-on-CPU model.
func (m *storageWatchManager) refreshWatchlistLocked() {
	union := make(map[common.Address]map[common.Hash]struct{})
	for _, sub := range m.subs {
		for addr, slots := range sub.filters {
			if slots == nil {
				union[addr] = nil // all slots wins over any specific set
				continue
			}
			existing, ok := union[addr]
			if ok && existing == nil {
				continue // already watching all slots of addr
			}
			if existing == nil {
				existing = make(map[common.Hash]struct{}, len(slots))
				union[addr] = existing
			}
			for slot := range slots {
				existing[slot] = struct{}{}
			}
		}
	}
	state.SetBlockswordsStorageWatchlist(union)
}

// startLocked starts the shared chain-head loop. Must hold m.mu.
func (m *storageWatchManager) startLocked() {
	headCh := make(chan blockchain.ChainHeadEvent, storageWatchHeadBacklog)
	headSub := m.backend.SubscribeChainHeadEvent(headCh)
	quit := make(chan struct{})
	m.quit = quit
	m.running = true

	go func() {
		defer headSub.Unsubscribe()
		for {
			select {
			case ev := <-headCh:
				m.dispatch(ev.Block)
			case <-headSub.Err():
				return
			case <-quit:
				return
			}
		}
	}()
}

// dispatch joins the captured delta for the new canonical head against the
// active subscriptions and delivers each its matching, filtered changes.
func (m *storageWatchManager) dispatch(block *types.Block) {
	number64 := block.NumberU64()
	// Detect a batch-insert head jump. A normal live insert advances the head one
	// block at a time (one ChainHeadEvent per block); a batch insert commits several
	// blocks but emits a single ChainHeadEvent for the last, so the per-root deltas of
	// the skipped blocks are never joined here. Silently delivering only the head's
	// delta would advance seq with no gap, hiding the skipped blocks from the client
	// and violating the no-silent-gap contract. Instead, re-anchor every affected
	// subscription below (a fresh "ready" so the client re-snapshots at the new head,
	// whose state already reflects the skipped blocks' cumulative effect, then resumes
	// the delta stream strictly after). lastDispatched is owned solely by this single
	// dispatch goroutine, so it needs no synchronization; it starts at zero (no real
	// block is 0) so the first dispatch is never treated as a jump.
	jumped := m.lastDispatched != 0 && number64 > m.lastDispatched+1
	m.lastDispatched = number64

	changes := state.LookupBlockswordsStorageChanges(block.Root())

	// Read the subscription set lock-free: dispatch must never block on m.mu, or a
	// long refreshWatchlistLocked rebuild during a subscribe burst could stall the
	// head loop and back-pressure the (block-import-critical) ChainHeadEvent feed.
	// dispatch is the only writer of each sub's anchored/seq, and runs solely on the
	// single head-loop goroutine, so a momentarily stale snapshot is harmless.
	var subs []*storageWatchSubscription
	if p := m.subsSnapshot.Load(); p != nil {
		subs = *p
	}

	number := hexutil.Uint64(number64)
	hash := block.Hash()
	for _, sub := range subs {
		// On a head jump, re-anchor an already-anchored subscription: drop it back to
		// un-anchored so the branch below re-emits a "ready" and the client
		// re-snapshots, rather than missing the skipped blocks' changes.
		if jumped && sub.anchored {
			sub.anchored = false
		}
		// Anchor a fresh (or re-anchoring) subscription: emit a "ready" frame naming
		// this block and skip this block's changes. The consumer snapshots pinned at
		// this block, and we then deliver every strictly-later block — no gap, no
		// overlap. This block's own capture may be partial (mid-commit when the
		// watch-list activated), which is why we exclude it from the delta stream.
		//
		// The "ready" must reach the client (it is the resync signal). Unlike the
		// initial anchor — a guaranteed-deliverable send to a fresh, empty buffer — a
		// RE-anchor can hit a full buffer (a slow consumer is exactly what makes a head
		// jump likely). So flip anchored (and reset seq) only when the ready was
		// actually delivered; on a drop the subscription stays un-anchored and the next
		// dispatch re-attempts the ready at the then-current head, mirroring the
		// reactive engine's emit→retry self-heal. This keeps the no-silent-gap
		// guarantee: a re-snapshot signal is never lost, only deferred until it lands.
		if !sub.anchored {
			if sub.emit(&StorageChangeNotification{
				Type:        StorageWatchReady,
				BlockNumber: number,
				BlockHash:   hash,
			}) {
				sub.anchored = true
				sub.seq = 0
			}
			continue
		}
		if len(changes) == 0 {
			continue
		}
		var matched []StorageChangeEntry
		for _, c := range changes {
			if sub.match(c) {
				matched = append(matched, StorageChangeEntry{
					Address:  c.Address,
					Key:      c.Key,
					Previous: c.Previous,
					Value:    c.Value,
				})
			}
		}
		if len(matched) == 0 {
			continue
		}
		sub.seq++
		sub.emit(&StorageChangeNotification{
			Type:        StorageWatchChanges,
			Seq:         hexutil.Uint64(sub.seq),
			BlockNumber: number,
			BlockHash:   hash,
			Changes:     matched,
		})
	}
}
