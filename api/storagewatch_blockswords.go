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

	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/state"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/networks/rpc"
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
				notifier.Notify(rpcSub.ID, notification)
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

// emit performs a non-blocking send. A full buffer means the consumer fell
// behind; the message is dropped (the already-advanced seq exposes the gap) and
// the subscription stays alive so the consumer can resync rather than being
// force-closed. We never block here: blocking would stall delivery to other
// subscribers and back-pressure the chain-head feed. The "ready" frame is the
// first send to a fresh, empty buffer, so it cannot be dropped.
func (sub *storageWatchSubscription) emit(n *StorageChangeNotification) {
	select {
	case sub.ch <- n:
	default:
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
	m.refreshWatchlistLocked()
	if len(m.subs) == 0 && m.running {
		m.running = false
		close(m.quit)
	}
}

// refreshWatchlistLocked recomputes the union of every subscription's filters
// (addr -> slot-set, nil set = all slots) and pushes it to the state-layer
// capture gate, so writes to slots no subscription cares about are skipped at
// capture time rather than buffered and filtered later. Must hold m.mu.
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
	changes := state.LookupBlockswordsStorageChanges(block.Root())

	m.mu.Lock()
	subs := make([]*storageWatchSubscription, 0, len(m.subs))
	for _, sub := range m.subs {
		subs = append(subs, sub)
	}
	m.mu.Unlock()

	number := hexutil.Uint64(block.NumberU64())
	hash := block.Hash()
	for _, sub := range subs {
		// Anchor a fresh subscription on the first head it observes: emit a
		// "ready" frame naming this block and skip this block's changes. The
		// consumer snapshots pinned at this block, and we then deliver every
		// strictly-later block — no gap, no overlap. This block's own capture
		// may be partial (it could have been mid-commit when the watch-list
		// activated), which is exactly why we exclude it from the delta stream
		// and let the consumer's snapshot cover it.
		if !sub.anchored {
			sub.anchored = true
			sub.emit(&StorageChangeNotification{
				Type:        StorageWatchReady,
				BlockNumber: number,
				BlockHash:   hash,
			})
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
