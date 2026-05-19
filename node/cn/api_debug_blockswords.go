// Modifications Copyright 2024 The Kaia Authors
// Modifications Copyright 2018 The klaytn Authors
// Copyright 2015 The go-ethereum Authors
// This file is part of go-ethereum.
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

package cn

// Blockswords fork-only debug APIs are kept in this file to minimize merge
// conflicts with upstream Kaia files.

import (
	"context"
	"fmt"

	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/networks/rpc"
)

type DiffContractStorageHashResult struct {
	FromBlock hexutil.Uint64                `json:"fromBlock"`
	ToBlock   hexutil.Uint64                `json:"toBlock"`
	Storage   map[string]map[string]*string `json:"storage"`
}

// DumpContractStorage retrieves the entire storage (key-value) of
// the contracts with given addresses at a given block.
func (api *DebugCNAPI) DumpContractStorage(ctx context.Context, addresses []common.Address, blockNumberOrHash rpc.BlockNumberOrHash) (map[string]map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	block, err := api.cn.APIBackend.BlockByNumberOrHash(ctx, blockNumberOrHash)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	stateDB, err := api.cn.BlockChain().StateAt(block.Root())
	if err != nil {
		return nil, err
	}

	res := map[string]map[string]string{}
	for _, address := range addresses {
		storage, err := stateDB.DumpContractStorage(ctx, address)
		if err != nil {
			return nil, err
		}
		res[address.String()] = storage
	}

	return res, nil
}

// DiffContractStorageHash retrieves changed contract storage entries between two blocks
// using hashed storage trie keys. If toBlock is nil, the latest block is used.
func (api *DebugCNAPI) DiffContractStorageHash(ctx context.Context, addresses []common.Address, fromBlock rpc.BlockNumberOrHash, toBlock *rpc.BlockNumberOrHash, includeDeletions *bool) (*DiffContractStorageHashResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	from, err := api.cn.APIBackend.BlockByNumberOrHash(ctx, fromBlock)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	toBlockSelector := rpc.NewBlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	if toBlock != nil {
		toBlockSelector = *toBlock
	}
	to, err := api.cn.APIBackend.BlockByNumberOrHash(ctx, toBlockSelector)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if from.NumberU64() >= to.NumberU64() {
		return nil, fmt.Errorf("from block height (%d) must be less than to block height (%d)", from.NumberU64(), to.NumberU64())
	}

	fromStateDB, err := api.cn.BlockChain().StateAt(from.Root())
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	toStateDB, err := api.cn.BlockChain().StateAt(to.Root())
	if err != nil {
		return nil, err
	}

	withDeletions := includeDeletions != nil && *includeDeletions
	res := &DiffContractStorageHashResult{
		FromBlock: hexutil.Uint64(from.NumberU64()),
		ToBlock:   hexutil.Uint64(to.NumberU64()),
		Storage:   map[string]map[string]*string{},
	}
	for _, address := range addresses {
		storage, err := toStateDB.DiffContractStorageHash(ctx, fromStateDB, address, withDeletions)
		if err != nil {
			return nil, err
		}
		res.Storage[address.String()] = storage
	}

	return res, nil
}

// DumpContractStorageHash retrieves the entire storage of the contracts with
// hashed storage trie keys. It skips preimage lookups for low-latency consumers.
func (api *DebugCNAPI) DumpContractStorageHash(ctx context.Context, addresses []common.Address, blockNumberOrHash rpc.BlockNumberOrHash) (map[string]map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	block, err := api.cn.APIBackend.BlockByNumberOrHash(ctx, blockNumberOrHash)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	stateDB, err := api.cn.BlockChain().StateAt(block.Root())
	if err != nil {
		return nil, err
	}

	res := map[string]map[string]string{}
	for _, address := range addresses {
		storage, err := stateDB.DumpContractStorageHash(ctx, address)
		if err != nil {
			return nil, err
		}
		res[address.String()] = storage
	}

	return res, nil
}
