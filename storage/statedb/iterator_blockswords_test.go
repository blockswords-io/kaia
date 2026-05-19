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

// Blockswords fork-only trie iterator tests are kept in this file to minimize
// merge conflicts with upstream Kaia files.

import (
	"context"
	"errors"
	"maps"
	"math/rand"
	"testing"
)

func TestBlockswordsDifferences(t *testing.T) {
	type diffItem struct {
		kind  BlockswordsDifferenceKind
		value string
	}
	collect := func(oldData, newData []kvs, includeDeletions bool) map[string]diffItem {
		oldTrie := newEmptyTrie()
		for _, val := range oldData {
			oldTrie.Update([]byte(val.k), []byte(val.v))
		}
		oldTrie.Commit(nil)

		newTrie := newEmptyTrie()
		for _, val := range newData {
			newTrie.Update([]byte(val.k), []byte(val.v))
		}
		newTrie.Commit(nil)

		found := make(map[string]diffItem)
		for diff, err := range BlockswordsDifferences(context.Background(), oldTrie.NodeIterator(nil), newTrie.NodeIterator(nil), includeDeletions) {
			if err != nil {
				t.Fatal(err)
			}
			found[string(diff.Key)] = diffItem{kind: diff.Kind, value: string(diff.Value)}
		}
		return found
	}

	t.Run("added and updated", func(t *testing.T) {
		found := collect(testdata1, testdata2, false)
		want := map[string]diffItem{
			"aardvark": {kind: BlockswordsDifferenceAddedOrUpdated, value: "c"},
			"barb":     {kind: BlockswordsDifferenceAddedOrUpdated, value: "bd"},
			"bars":     {kind: BlockswordsDifferenceAddedOrUpdated, value: "be"},
			"jars":     {kind: BlockswordsDifferenceAddedOrUpdated, value: "d"},
		}
		if !maps.Equal(found, want) {
			t.Fatalf("diff mismatch: got %v want %v", found, want)
		}
	})

	t.Run("added updated and deleted", func(t *testing.T) {
		found := collect(testdata1, testdata2, true)
		want := map[string]diffItem{
			"aardvark": {kind: BlockswordsDifferenceAddedOrUpdated, value: "c"},
			"barb":     {kind: BlockswordsDifferenceAddedOrUpdated, value: "bd"},
			"bard":     {kind: BlockswordsDifferenceDeleted, value: "bc"},
			"bars":     {kind: BlockswordsDifferenceAddedOrUpdated, value: "be"},
			"jars":     {kind: BlockswordsDifferenceAddedOrUpdated, value: "d"},
		}
		if !maps.Equal(found, want) {
			t.Fatalf("diff mismatch: got %v want %v", found, want)
		}
	})

	t.Run("leaf split is not deletion", func(t *testing.T) {
		found := collect(
			[]kvs{{"abc", "x"}},
			[]kvs{{"abc", "x"}, {"abd", "y"}},
			true,
		)
		want := map[string]diffItem{
			"abd": {kind: BlockswordsDifferenceAddedOrUpdated, value: "y"},
		}
		if !maps.Equal(found, want) {
			t.Fatalf("diff mismatch: got %v want %v", found, want)
		}
	})

	t.Run("leaf merge is deletion", func(t *testing.T) {
		found := collect(
			[]kvs{{"abc", "x"}, {"abd", "y"}},
			[]kvs{{"abc", "x"}},
			true,
		)
		want := map[string]diffItem{
			"abd": {kind: BlockswordsDifferenceDeleted, value: "y"},
		}
		if !maps.Equal(found, want) {
			t.Fatalf("diff mismatch: got %v want %v", found, want)
		}
	})

	t.Run("context error", func(t *testing.T) {
		trie := newEmptyTrie()
		trie.Update([]byte("abc"), []byte("x"))
		trie.Commit(nil)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var gotErr error
		for diff, err := range BlockswordsDifferences(ctx, trie.NodeIterator(nil), trie.NodeIterator(nil), true) {
			if err == nil {
				t.Fatalf("unexpected diff: key %q value %q", diff.Key, diff.Value)
			}
			gotErr = err
		}
		if !errors.Is(gotErr, context.Canceled) {
			t.Fatalf("got error %v, want %v", gotErr, context.Canceled)
		}
	})
}

func TestBlockswordsDifferencesMatchesTwoPass(t *testing.T) {
	type diffItem struct {
		deleted bool
		value   string
	}
	collectTwoPass := func(oldTrie, newTrie *Trie, includeDeletions bool) map[string]diffItem {
		found := make(map[string]diffItem)
		diff, _ := NewDifferenceIterator(oldTrie.NodeIterator(nil), newTrie.NodeIterator(nil))
		it := NewIterator(diff)
		for it.Next() {
			found[string(it.Key)] = diffItem{value: string(it.Value)}
		}
		if it.Err != nil {
			t.Fatal(it.Err)
		}
		if includeDeletions {
			diff, _ = NewDifferenceIterator(newTrie.NodeIterator(nil), oldTrie.NodeIterator(nil))
			it = NewIterator(diff)
			for it.Next() {
				if _, ok := found[string(it.Key)]; !ok {
					found[string(it.Key)] = diffItem{deleted: true, value: string(it.Value)}
				}
			}
			if it.Err != nil {
				t.Fatal(it.Err)
			}
		}
		return found
	}
	collectSinglePass := func(oldTrie, newTrie *Trie, includeDeletions bool) map[string]diffItem {
		found := make(map[string]diffItem)
		for diff, err := range BlockswordsDifferences(context.Background(), oldTrie.NodeIterator(nil), newTrie.NodeIterator(nil), includeDeletions) {
			if err != nil {
				t.Fatal(err)
			}
			found[string(diff.Key)] = diffItem{
				deleted: diff.Kind == BlockswordsDifferenceDeleted,
				value:   string(diff.Value),
			}
		}
		return found
	}

	rng := rand.New(rand.NewSource(0))
	for round := 0; round < 100; round++ {
		oldTrie := newEmptyTrie()
		newTrie := newEmptyTrie()

		for i := 0; i < 250; i++ {
			key := make([]byte, 4)
			value := make([]byte, 4)
			rng.Read(key)
			rng.Read(value)

			oldTrie.Update(key, value)
			switch rng.Intn(4) {
			case 0:
				// deleted
			case 1:
				updated := append([]byte(nil), value...)
				updated[0] ^= 0xff
				newTrie.Update(key, updated)
			default:
				newTrie.Update(key, value)
			}
		}
		for i := 0; i < 50; i++ {
			key := make([]byte, 4)
			value := make([]byte, 4)
			rng.Read(key)
			rng.Read(value)
			newTrie.Update(key, value)
		}
		oldTrie.Commit(nil)
		newTrie.Commit(nil)

		for _, includeDeletions := range []bool{false, true} {
			twoPass := collectTwoPass(oldTrie, newTrie, includeDeletions)
			singlePass := collectSinglePass(oldTrie, newTrie, includeDeletions)
			if !maps.Equal(singlePass, twoPass) {
				t.Fatalf("round %d includeDeletions=%v mismatch: got %v want %v", round, includeDeletions, singlePass, twoPass)
			}
		}
	}
}
