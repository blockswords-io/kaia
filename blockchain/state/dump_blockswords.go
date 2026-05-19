// Modifications Copyright 2024 The Kaia Authors
// Modifications Copyright 2018 The klaytn Authors
// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package state

// Blockswords fork-only state helpers are kept in this file to minimize merge
// conflicts with upstream Kaia files.

import (
	"context"
	"errors"

	"github.com/kaiachain/kaia/blockchain/types/account"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/storage/statedb"
)

func (self *StateDB) DumpContractStorage(ctx context.Context, address common.Address) (map[string]string, error) {
	return self.dumpContractStorage(ctx, address, true)
}

func (self *StateDB) DumpContractStorageHash(ctx context.Context, address common.Address) (map[string]string, error) {
	return self.dumpContractStorage(ctx, address, false)
}

func (self *StateDB) DiffContractStorageHash(ctx context.Context, previous *StateDB, address common.Address, includeDeletions bool) (map[string]*string, error) {
	res := map[string]*string{}
	if previous == nil {
		return res, errors.New("previous StateDB is nil")
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}

	oldObj := previous.getStateObject(address)
	newObj := self.getStateObject(address)
	if contractStorageRoot(oldObj) == contractStorageRoot(newObj) {
		return res, nil
	}

	oldTrie, err := contractStorageTrie(previous, address, oldObj)
	if err != nil {
		return res, err
	}
	newTrie, err := contractStorageTrie(self, address, newObj)
	if err != nil {
		return res, err
	}

	if err := diffContractStorageHash(ctx, oldTrie, newTrie, res, includeDeletions); err != nil {
		return res, err
	}
	return res, nil
}

func (self *StateDB) dumpContractStorage(ctx context.Context, address common.Address, resolveKeys bool) (map[string]string, error) {
	res := map[string]string{}

	obj := self.getStateObject(address)
	if obj == nil {
		return res, nil
	}
	if err := ctx.Err(); err != nil {
		return res, err
	}
	trie := obj.getStorageTrie(self.db)
	it := statedb.NewIterator(trie.NodeIterator(nil))
	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		if !it.Next() {
			break
		}
		var key string
		if resolveKeys {
			resolvedKey, err := resolveTrieKey(ctx, trie, it.Key)
			if err != nil {
				return res, err
			}
			key = common.Bytes2Hex(resolvedKey)
		}
		if key == "" {
			key = "hash:" + common.Bytes2Hex(it.Key)
		}
		res[key] = common.Bytes2Hex(it.Value)
	}
	if it.Err != nil {
		return res, it.Err
	}

	return res, nil
}

func contractStorageRoot(obj *stateObject) common.ExtHash {
	if obj == nil {
		return common.ExtHash{}
	}
	if pa := account.GetProgramAccount(obj.account); pa != nil {
		return pa.GetStorageRoot()
	}
	return common.ExtHash{}
}

func contractStorageTrie(stateDB *StateDB, address common.Address, obj *stateObject) (Trie, error) {
	if obj != nil {
		return obj.getStorageTrie(stateDB.db), nil
	}
	return stateDB.db.OpenStorageTrie(address, common.ExtHash{}, stateDB.trieOpts)
}

func diffContractStorageHash(ctx context.Context, oldTrie, newTrie Trie, res map[string]*string, includeDeletions bool) error {
	for diff, err := range statedb.BlockswordsDifferences(ctx, oldTrie.NodeIterator(nil), newTrie.NodeIterator(nil), includeDeletions) {
		if err != nil {
			return err
		}
		key := "hash:" + common.Bytes2Hex(diff.Key)
		if diff.Kind == statedb.BlockswordsDifferenceDeleted {
			res[key] = nil
			continue
		}
		value := common.Bytes2Hex(diff.Value)
		res[key] = &value
	}
	return nil
}

func resolveTrieKey(ctx context.Context, trie Trie, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return trie.GetKey(key), nil
}

// GetStates retrieves values from the given account's storage trie.
func (self *StateDB) GetStates(addr common.Address, hashes []common.Hash) map[common.Hash]common.Hash {
	states := map[common.Hash]common.Hash{}
	stateObject := self.getStateObject(addr)
	if stateObject != nil {
		for _, hash := range hashes {
			state := stateObject.GetState(self.db, hash)
			if !common.EmptyHash(state) {
				states[hash] = state
			}
		}
	}
	return states
}
