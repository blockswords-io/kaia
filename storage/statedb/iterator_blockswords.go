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

package statedb

// Blockswords fork-only trie iterator helpers are kept in this file to minimize
// merge conflicts with upstream Kaia files.

import (
	"bytes"
	"context"
	"iter"

	"github.com/kaiachain/kaia/common"
)

// BlockswordsDifferenceKind describes how a trie leaf changed between two iterators.
type BlockswordsDifferenceKind uint8

const (
	// BlockswordsDifferenceAddedOrUpdated means a leaf exists in the new trie and either
	// did not exist in the old trie or has a different value.
	BlockswordsDifferenceAddedOrUpdated BlockswordsDifferenceKind = iota

	// BlockswordsDifferenceDeleted means a leaf exists in the old trie but not in the new trie.
	BlockswordsDifferenceDeleted
)

// BlockswordsDifference describes one changed leaf between two tries.
//
// Key and Value are only valid until the next yielded item. Added and updated
// differences carry the value from the new trie. Deleted differences carry the
// value from the old trie.
type BlockswordsDifference struct {
	Kind  BlockswordsDifferenceKind
	Key   []byte
	Value []byte
}

// BlockswordsDifferences returns a single-use iterator over changed leaves.
//
// Added and updated leaves are emitted from the new trie. Deleted leaves are
// emitted from the old trie when includeDeletions is true. Context and trie
// iterator errors are yielded as the second value and stop the sequence.
func BlockswordsDifferences(ctx context.Context, oldIt, newIt NodeIterator, includeDeletions bool) iter.Seq2[BlockswordsDifference, error] {
	if includeDeletions {
		return blockswordsDifferencesWithDeletions(ctx, oldIt, newIt)
	}
	return blockswordsForwardDifferences(ctx, oldIt, newIt)
}

type blockswordsDifferenceSource interface {
	next() (BlockswordsDifference, bool)
	terminalError() error
}

func blockswordsForwardDifferences(ctx context.Context, oldIt, newIt NodeIterator) iter.Seq2[BlockswordsDifference, error] {
	return blockswordsDifferenceSequence(ctx, func() blockswordsDifferenceSource {
		return newBlockswordsForwardDifferenceCursor(oldIt, newIt)
	})
}

func blockswordsDifferencesWithDeletions(ctx context.Context, oldIt, newIt NodeIterator) iter.Seq2[BlockswordsDifference, error] {
	return blockswordsDifferenceSequence(ctx, func() blockswordsDifferenceSource {
		return newBlockswordsDifferenceCursor(oldIt, newIt)
	})
}

func blockswordsDifferenceSequence(ctx context.Context, newSource func() blockswordsDifferenceSource) iter.Seq2[BlockswordsDifference, error] {
	return func(yield func(BlockswordsDifference, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(BlockswordsDifference{}, err)
			return
		}
		source := newSource()
		for {
			if err := ctx.Err(); err != nil {
				yield(BlockswordsDifference{}, err)
				return
			}
			item, ok := source.next()
			if !ok {
				break
			}
			if !yield(item, nil) {
				return
			}
		}
		if err := blockswordsIteratorError(ctx, source.terminalError()); err != nil {
			yield(BlockswordsDifference{}, err)
		}
	}
}

type blockswordsForwardDifferenceCursor struct {
	it *Iterator
}

func newBlockswordsForwardDifferenceCursor(oldIt, newIt NodeIterator) *blockswordsForwardDifferenceCursor {
	diff, _ := NewDifferenceIterator(oldIt, newIt)
	return &blockswordsForwardDifferenceCursor{
		it: NewIterator(diff),
	}
}

func (it *blockswordsForwardDifferenceCursor) next() (BlockswordsDifference, bool) {
	if !it.it.Next() {
		return BlockswordsDifference{}, false
	}
	return BlockswordsDifference{
		Kind:  BlockswordsDifferenceAddedOrUpdated,
		Key:   it.it.Key,
		Value: it.it.Value,
	}, true
}

func (it *blockswordsForwardDifferenceCursor) terminalError() error {
	return it.it.Err
}

func blockswordsIteratorError(ctx context.Context, err error) error {
	if err != nil {
		return err
	}
	return ctx.Err()
}

// blockswordsDifferenceCursor walks two tries together and yields changed leaves.
//
// Added and updated leaves are emitted from the new trie. Deleted leaves are
// emitted from the old trie. Equal hashed subtrees are skipped without
// descending into them.
type blockswordsDifferenceCursor struct {
	oldIt, newIt NodeIterator

	oldOK, newOK bool
	started      bool
	done         bool
	advanceOld   bool
	advanceNew   bool

	err error
}

func newBlockswordsDifferenceCursor(oldIt, newIt NodeIterator) *blockswordsDifferenceCursor {
	return &blockswordsDifferenceCursor{
		oldIt: oldIt,
		newIt: newIt,
	}
}

func (it *blockswordsDifferenceCursor) terminalError() error {
	return it.err
}

func (it *blockswordsDifferenceCursor) next() (BlockswordsDifference, bool) {
	if it.done {
		return BlockswordsDifference{}, false
	}

	if !it.started {
		it.oldOK = it.oldIt.Next(true)
		it.newOK = it.newIt.Next(true)
		it.started = true
	} else {
		it.advance()
	}

	for it.oldOK || it.newOK {
		switch {
		case !it.oldOK:
			if it.newIt.Leaf() {
				return it.emit(BlockswordsDifferenceAddedOrUpdated, it.newIt.LeafKey(), it.newIt.LeafBlob(), false, true)
			}
			it.newOK = it.newIt.Next(true)

		case !it.newOK:
			if it.oldIt.Leaf() {
				return it.emit(BlockswordsDifferenceDeleted, it.oldIt.LeafKey(), it.oldIt.LeafBlob(), true, false)
			}
			it.oldOK = it.oldIt.Next(true)

		default:
			if it.oldIt.Leaf() && it.newIt.Leaf() {
				switch cmp := bytes.Compare(it.oldIt.LeafKey(), it.newIt.LeafKey()); {
				case cmp < 0:
					return it.emit(BlockswordsDifferenceDeleted, it.oldIt.LeafKey(), it.oldIt.LeafBlob(), true, false)
				case cmp > 0:
					return it.emit(BlockswordsDifferenceAddedOrUpdated, it.newIt.LeafKey(), it.newIt.LeafBlob(), false, true)
				default:
					if !bytes.Equal(it.oldIt.LeafBlob(), it.newIt.LeafBlob()) {
						return it.emit(BlockswordsDifferenceAddedOrUpdated, it.newIt.LeafKey(), it.newIt.LeafBlob(), true, true)
					}
					it.oldOK = it.oldIt.Next(true)
					it.newOK = it.newIt.Next(true)
				}
				continue
			}

			if !it.oldIt.Leaf() && !it.newIt.Leaf() && bytes.Equal(it.oldIt.Path(), it.newIt.Path()) {
				descend := true
				if it.oldIt.Hash() != (common.Hash{}) && it.oldIt.Hash() == it.newIt.Hash() {
					descend = false
				}
				it.oldOK = it.oldIt.Next(descend)
				it.newOK = it.newIt.Next(descend)
				continue
			}

			switch cmp := compareBlockswordsNodePosition(it.oldIt, it.newIt); {
			case cmp < 0:
				if it.oldIt.Leaf() {
					return it.emit(BlockswordsDifferenceDeleted, it.oldIt.LeafKey(), it.oldIt.LeafBlob(), true, false)
				}
				it.oldOK = it.oldIt.Next(true)
			case cmp > 0:
				if it.newIt.Leaf() {
					return it.emit(BlockswordsDifferenceAddedOrUpdated, it.newIt.LeafKey(), it.newIt.LeafBlob(), false, true)
				}
				it.newOK = it.newIt.Next(true)
			default:
				it.oldOK = it.oldIt.Next(true)
				it.newOK = it.newIt.Next(true)
			}
		}
	}
	return it.finish()
}

func (it *blockswordsDifferenceCursor) finish() (BlockswordsDifference, bool) {
	it.done = true
	it.err = blockswordsDifferenceError(it.oldIt, it.newIt)
	return BlockswordsDifference{}, false
}

func (it *blockswordsDifferenceCursor) emit(kind BlockswordsDifferenceKind, key, value []byte, advanceOld, advanceNew bool) (BlockswordsDifference, bool) {
	it.advanceOld = advanceOld
	it.advanceNew = advanceNew
	return BlockswordsDifference{
		Kind:  kind,
		Key:   key,
		Value: value,
	}, true
}

func (it *blockswordsDifferenceCursor) advance() {
	if it.advanceOld {
		it.oldOK = it.oldIt.Next(true)
		it.advanceOld = false
	}
	if it.advanceNew {
		it.newOK = it.newIt.Next(true)
		it.advanceNew = false
	}
}

func compareBlockswordsNodePosition(a, b NodeIterator) int {
	if cmp := bytes.Compare(a.Path(), b.Path()); cmp != 0 {
		return cmp
	}
	if a.Leaf() && !b.Leaf() {
		return -1
	} else if b.Leaf() && !a.Leaf() {
		return 1
	}
	return 0
}

func blockswordsDifferenceError(oldIt, newIt NodeIterator) error {
	if err := oldIt.Error(); err != nil {
		return err
	}
	return newIt.Error()
}
