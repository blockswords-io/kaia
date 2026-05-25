// Modifications Copyright 2024 The Kaia Authors
// Modifications Copyright 2019 The klaytn Authors
// Copyright 2015 The go-ethereum Authors
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

package api

// Blockswords fork-only APIs are kept in this file to minimize merge conflicts
// with upstream Kaia files.

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/blockchain/vm"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/crypto/sha3"
	"github.com/kaiachain/kaia/networks/rpc"
	"github.com/kaiachain/kaia/params"
)

var registeredStorageKeysSet = sync.Map{}

// RegisterStorageKeys registers the storage keys to be used in the GetStoragesAt API.
func (s *KaiaBlockChainAPI) RegisterStorageKeys(ctx context.Context, keys []common.Hash) bool {
	var hash common.Hash

	d := sha3.NewKeccak256()
	for _, key := range keys {
		d.Write(key[:])
	}
	d.Sum(hash[:0])

	registeredKeys := make([]common.Hash, len(keys))
	copy(registeredKeys, keys)
	registeredStorageKeysSet.Store(hash, registeredKeys)

	return true
}

// RegisteredStorageKeyHashes returns the registered storage key hashes.
func (s *KaiaBlockChainAPI) RegisteredStorageKeyHashes(ctx context.Context) []common.Hash {
	hashes := make([]common.Hash, 0)
	invalidHashes := make([]common.Hash, 0)

	registeredStorageKeysSet.Range(func(key, value interface{}) bool {
		hash, ok := key.(common.Hash)
		if !ok {
			invalidHashes = append(invalidHashes, hash)
			return true
		}

		hashes = append(hashes, hash)

		return true
	})

	for _, hash := range invalidHashes {
		registeredStorageKeysSet.Delete(hash)
	}

	return hashes
}

// UnregisterStorageKeyHash unregisters the storage key hash.
func (s *KaiaBlockChainAPI) UnregisterStorageKeyHash(ctx context.Context, hash common.Hash) bool {
	registeredStorageKeysSet.Delete(hash)
	return true
}

// GetRegisteredStoragesAt returns the storages from the state at the given address,
// registered storage keys and block number. The rpc.LatestBlockNumber and rpc.PendingBlockNumber
// meta block numbers and hash are also allowed.
func (s *KaiaBlockChainAPI) GetRegisteredStoragesAt(ctx context.Context, address common.Address, hash common.Hash, blockNrOrHash rpc.BlockNumberOrHash) (map[common.Hash]common.Hash, error) {
	o, ok := registeredStorageKeysSet.Load(hash)
	if !ok {
		return nil, fmt.Errorf("storage keys not found")
	}

	keys, ok := o.([]common.Hash)
	if !ok {
		registeredStorageKeysSet.Delete(hash)
		return nil, fmt.Errorf("invalid storage keys")
	}

	return s.GetStoragesAt(ctx, address, keys, blockNrOrHash)
}

// GetStoragesAt returns the storages from the state at the given address, keys and
// block number. The rpc.LatestBlockNumber and rpc.PendingBlockNumber meta block
// numbers and hash are also allowed.
func (s *KaiaBlockChainAPI) GetStoragesAt(ctx context.Context, address common.Address, keys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash) (map[common.Hash]common.Hash, error) {
	state, _, err := s.b.StateAndHeaderByNumberOrHash(ctx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	res := state.GetStates(address, keys)
	return res, state.Error()
}

type EstimateGasTraceResult struct {
	// Gas is an estimate of the amount of gas needed to execute the given transaction against the latest block.
	Gas hexutil.Uint64 `json:"gas"`
	// Trace contains trace result. Empty (undefined) Trace and empty error means estimation was successful.
	Trace interface{} `json:"trace,omitempty"`
	// Error is a raw error returned from the vm.
	Error *string `json:"error,omitempty"`
}

// EstimateGasWithTrace returns an estimated gas and trace if it is going to be reverted.
// NOTE: This method is an unofficial, blockswords fork only method to help debugging.
func (s *KaiaBlockChainAPI) EstimateGasWithTrace(ctx context.Context, args CallArgs) (*EstimateGasTraceResult, error) {
	estimatedGas, err := s.EstimateGas(ctx, args, nil, nil)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		errStr := err.Error()

		tracer := vm.NewInternalTxTracer()

		gasCap := new(big.Int)
		if rpcGasCap := s.b.RPCGasCap(); rpcGasCap != nil {
			gasCap.Set(rpcGasCap)
		}
		upperGas := hexutil.Uint64(params.UpperGasLimit)
		args.Gas = &upperGas
		latestBlockNumberOrHash := rpc.NewBlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
		DoCall(ctx, s.b, args, latestBlockNumberOrHash, vm.Config{Debug: true, Tracer: tracer, ComputationCostLimit: params.OpcodeComputationCostLimitInfinite, UseConsoleLog: s.b.IsConsoleLogEnabled()}, s.b.RPCEVMTimeout(), gasCap)

		tracerResult, tracerErr := tracer.GetResult()
		return &EstimateGasTraceResult{
			Gas:   estimatedGas,
			Trace: tracerResult,
			Error: &errStr,
		}, tracerErr
	}

	return &EstimateGasTraceResult{Gas: estimatedGas}, nil
}

// CallWithAccessedStorageResult is the result of CallWithAccessedStorage: the
// call's return data plus the access list (addresses and storage slots) the EVM
// read or wrote during execution.
//
// AccessList tuples that carry storage keys identify the contracts — and the
// specific slots — whose storage this call depends on. They can be fed directly
// into kaia_subscribe("storageChanges") to invalidate this call's cached result
// when its inputs change: use the tuple addresses for contract-granularity
// watching, or the tuple storage keys for slot-granularity watching.
type CallWithAccessedStorageResult struct {
	// Return is the call's return data (empty if the call reverted or errored).
	Return hexutil.Bytes `json:"return"`
	// GasUsed is the gas consumed by the call.
	GasUsed hexutil.Uint64 `json:"gasUsed"`
	// AccessList is the set of addresses and storage slots the call touched
	// (read or wrote), in EIP-2930 form. It is returned even when the call
	// reverts, reflecting the slots touched up to the point of revert.
	AccessList types.AccessList `json:"accessList"`
	// BalanceAddresses are the addresses whose balance the call read
	// (BALANCE/SELFBALANCE). Balances change outside the storage trie, so to
	// keep this call's result live these must be watched at account granularity.
	BalanceAddresses []common.Address `json:"balanceAddresses,omitempty"`
	// BlockContext lists the block-context opcodes the call used (e.g.
	// TIMESTAMP, NUMBER, BASEFEE). When non-empty the result may change every
	// block independently of state, so it cannot be tracked by watching state.
	BlockContext []string `json:"blockContext,omitempty"`
	// Trackable is true iff the result is a pure function of tracked state
	// (storage + balances) — i.e. BlockContext is empty. A reactive ("live")
	// subscription on this call is only sound when Trackable is true.
	Trackable bool `json:"trackable"`
	// Error is the raw VM error (e.g. "execution reverted"), if any.
	Error string `json:"error,omitempty"`
}

// CallWithAccessedStorage executes args exactly like kaia_call but, in addition
// to the return data, reports every (contract address, storage slot) the EVM
// read or wrote during the call. It is the discovery counterpart to
// kaia_subscribe("storageChanges"): run this once to learn which storage a call
// depends on, then watch those contracts/slots.
//
// NOTE: This method is an unofficial, blockswords fork only method.
func (s *KaiaBlockChainAPI) CallWithAccessedStorage(ctx context.Context, args CallArgs, blockNrOrHash rpc.BlockNumberOrHash) (*CallWithAccessedStorageResult, error) {
	gasCap := big.NewInt(0)
	if rpcGasCap := s.b.RPCGasCap(); rpcGasCap != nil {
		gasCap = rpcGasCap
	}

	// A single execution with the dependency tracer is enough to record the
	// touched (address, slot) set, the balance reads, and any block-context
	// reads; unlike eth_createAccessList we do not iterate to a gas fixpoint
	// because we only care about what the call depends on.
	tracer := vm.NewBlockswordsCallDependencyTracer(nil)
	vmCfg := vm.Config{
		Debug:                true,
		Tracer:               tracer,
		ComputationCostLimit: params.OpcodeComputationCostLimitInfinite,
		UseConsoleLog:        s.b.IsConsoleLogEnabled(),
	}
	result, _, err := DoCall(ctx, s.b, args, blockNrOrHash, vmCfg, s.b.RPCEVMTimeout(), gasCap)
	if err != nil {
		return nil, err
	}

	res := &CallWithAccessedStorageResult{
		Return:           result.Return(),
		GasUsed:          hexutil.Uint64(result.UsedGas),
		AccessList:       tracer.AccessList(),
		BalanceAddresses: tracer.BalanceAddresses(),
		BlockContext:     tracer.BlockContextOpcodes(),
		Trackable:        tracer.Trackable(),
	}
	if vmErr := result.Unwrap(); vmErr != nil {
		res.Error = vmErr.Error()
	}
	return res, nil
}
