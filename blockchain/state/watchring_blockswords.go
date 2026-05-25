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

// Blockswords fork-only bounded "watch ring" shared by the storage-write and
// account-change watches (storagewatch_blockswords.go, accountwatch_blockswords.go).
// It maps a state root to the per-block changes published at Commit; the RPC
// layer consumes an entry (delete-on-read) when it joins the root against the
// canonical ChainHeadEvent. Entries are normally consumed one block after they
// are published, so the live set is tiny; the capacity only guards against a
// momentarily absent consumer.

import (
	"sync"

	"github.com/kaiachain/kaia/common"
)

// blockswordsWatchRing is a bounded map of state root -> a slice of changes.
type blockswordsWatchRing[E any] struct {
	mu       sync.Mutex
	capacity int
	byRoot   map[common.Hash][]E
	order    []common.Hash
}

func newBlockswordsWatchRing[E any](capacity int) *blockswordsWatchRing[E] {
	return &blockswordsWatchRing[E]{
		capacity: capacity,
		byRoot:   make(map[common.Hash][]E),
	}
}

// publish stores the changes for a root, evicting the oldest roots beyond the
// capacity. Empty change sets are not stored.
func (r *blockswordsWatchRing[E]) publish(root common.Hash, items []E) {
	if len(items) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byRoot[root]; !exists {
		r.order = append(r.order, root)
		for len(r.order) > r.capacity {
			oldest := r.order[0]
			r.order = r.order[1:]
			delete(r.byRoot, oldest)
		}
	}
	r.byRoot[root] = items
}

// lookup returns and removes the changes for a root, or nil if none.
func (r *blockswordsWatchRing[E]) lookup(root common.Hash) []E {
	r.mu.Lock()
	defer r.mu.Unlock()
	items, ok := r.byRoot[root]
	if !ok {
		return nil
	}
	delete(r.byRoot, root)
	return items
}
