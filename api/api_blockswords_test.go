package api

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	mock_api "github.com/kaiachain/kaia/api/mocks"
	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/state"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/blockchain/vm"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/crypto/sha3"
	"github.com/kaiachain/kaia/networks/rpc"
	"github.com/kaiachain/kaia/params"
	"github.com/kaiachain/kaia/storage/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// computeStorageKeyHash replicates the hash computation done by RegisterStorageKeys.
func computeStorageKeyHash(keys []common.Hash) common.Hash {
	var hash common.Hash
	d := sha3.NewKeccak256()
	for _, key := range keys {
		d.Write(key[:])
	}
	d.Sum(hash[:0])
	return hash
}

// setupStorageTestBackend creates a mock backend with a genesis state containing
// an account with known storage entries.
func setupStorageTestBackend(t *testing.T) (
	*gomock.Controller, *mock_api.MockBackend, *KaiaBlockChainAPI,
	common.Address, common.Hash, common.Hash, common.Hash, common.Hash,
) {
	mockCtrl := gomock.NewController(t)
	mockBackend := mock_api.NewMockBackend(mockCtrl)
	blockchain.InitDeriveSha(params.TestChainConfig)
	api := NewKaiaBlockChainAPI(mockBackend)

	chainConfig := &params.ChainConfig{}
	chainConfig.IstanbulCompatibleBlock = common.Big0
	chainConfig.LondonCompatibleBlock = common.Big0
	chainConfig.EthTxTypeCompatibleBlock = common.Big0
	chainConfig.MagmaCompatibleBlock = common.Big0
	chainConfig.KoreCompatibleBlock = common.Big0
	chainConfig.ShanghaiCompatibleBlock = common.Big0
	chainConfig.CancunCompatibleBlock = common.Big0
	chainConfig.KaiaCompatibleBlock = common.Big0

	addr := common.HexToAddress("0x1234")
	key1 := common.HexToHash("0x01")
	key2 := common.HexToHash("0x02")
	val1 := common.HexToHash("0xaa")
	val2 := common.HexToHash("0xbb")

	gspec := &blockchain.Genesis{
		Alloc: blockchain.GenesisAlloc{
			addr: {
				Balance: big.NewInt(1),
				Storage: map[common.Hash]common.Hash{
					key1: val1,
					key2: val2,
				},
			},
		},
		Config: chainConfig,
	}

	dbm := database.NewMemoryDBManager()
	db := state.NewDatabase(dbm)
	block := gspec.MustCommit(dbm)
	header := block.Header()

	any := gomock.Any()
	getStateAndHeader := func(...interface{}) (*state.StateDB, *types.Header, error) {
		st, err := state.New(block.Root(), db, nil, nil)
		return st, header, err
	}

	mockBackend.EXPECT().ChainConfig().Return(chainConfig).AnyTimes()
	mockBackend.EXPECT().StateAndHeaderByNumberOrHash(any, any).DoAndReturn(getStateAndHeader).AnyTimes()

	return mockCtrl, mockBackend, api, addr, key1, key2, val1, val2
}

// clearRegisteredStorageKeys removes all entries from the global sync.Map
// so tests don't interfere with each other.
func clearRegisteredStorageKeys() {
	registeredStorageKeysSet.Range(func(key, value interface{}) bool {
		registeredStorageKeysSet.Delete(key)
		return true
	})
}

func TestRegisterStorageKeys(t *testing.T) {
	mockCtrl, _, api, _, _, _, _, _ := setupStorageTestBackend(t)
	defer mockCtrl.Finish()
	defer clearRegisteredStorageKeys()

	ctx := context.Background()
	key1 := common.HexToHash("0x01")
	key2 := common.HexToHash("0x02")

	t.Run("registers keys and returns true", func(t *testing.T) {
		result := api.RegisterStorageKeys(ctx, []common.Hash{key1, key2})
		assert.True(t, result)
	})

	t.Run("registered hash matches expected keccak256", func(t *testing.T) {
		clearRegisteredStorageKeys()
		api.RegisterStorageKeys(ctx, []common.Hash{key1, key2})

		expectedHash := computeStorageKeyHash([]common.Hash{key1, key2})
		// Verify via RegisteredStorageKeyHashes
		hashes := api.RegisteredStorageKeyHashes(ctx)
		require.Len(t, hashes, 1)
		assert.Equal(t, expectedHash, hashes[0])
		// Cross-check with precomputed value from v2.0.3
		assert.Equal(t,
			common.HexToHash("0xe90b7bceb6e7df5418fb78d8ee546e97c83a08bbccc01a0644d599ccd2a7c2e0"),
			hashes[0],
		)
	})

	t.Run("single key hash matches expected", func(t *testing.T) {
		clearRegisteredStorageKeys()
		api.RegisterStorageKeys(ctx, []common.Hash{key1})

		hashes := api.RegisteredStorageKeyHashes(ctx)
		require.Len(t, hashes, 1)
		assert.Equal(t,
			common.HexToHash("0xb10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf6"),
			hashes[0],
		)
	})

	t.Run("multiple registrations produce multiple hashes", func(t *testing.T) {
		clearRegisteredStorageKeys()
		api.RegisterStorageKeys(ctx, []common.Hash{key1})
		api.RegisterStorageKeys(ctx, []common.Hash{key2})
		api.RegisterStorageKeys(ctx, []common.Hash{key1, key2})

		hashes := api.RegisteredStorageKeyHashes(ctx)
		assert.Len(t, hashes, 3)
	})

	t.Run("copies registered keys", func(t *testing.T) {
		clearRegisteredStorageKeys()
		keys := []common.Hash{key1, key2}
		hash := computeStorageKeyHash(keys)

		api.RegisterStorageKeys(ctx, keys)
		keys[0] = common.HexToHash("0xff")

		stored, ok := registeredStorageKeysSet.Load(hash)
		require.True(t, ok)
		storedKeys, ok := stored.([]common.Hash)
		require.True(t, ok)
		assert.Equal(t, []common.Hash{key1, key2}, storedKeys)
	})
}

func TestUnregisterStorageKeyHash(t *testing.T) {
	mockCtrl, _, api, _, _, _, _, _ := setupStorageTestBackend(t)
	defer mockCtrl.Finish()
	defer clearRegisteredStorageKeys()

	ctx := context.Background()
	key1 := common.HexToHash("0x01")

	api.RegisterStorageKeys(ctx, []common.Hash{key1})
	hashes := api.RegisteredStorageKeyHashes(ctx)
	require.Len(t, hashes, 1)

	result := api.UnregisterStorageKeyHash(ctx, hashes[0])
	assert.True(t, result)

	hashes = api.RegisteredStorageKeyHashes(ctx)
	assert.Len(t, hashes, 0)
}

func TestGetStoragesAt(t *testing.T) {
	mockCtrl, _, api, addr, key1, key2, val1, val2 := setupStorageTestBackend(t)
	defer mockCtrl.Finish()

	ctx := context.Background()
	blockNrOrHash := rpc.NewBlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	t.Run("returns storage values for multiple keys", func(t *testing.T) {
		result, err := api.GetStoragesAt(ctx, addr, []common.Hash{key1, key2}, blockNrOrHash)
		require.NoError(t, err)
		assert.Equal(t, val1, result[key1])
		assert.Equal(t, val2, result[key2])
	})

	t.Run("omits empty storage slots", func(t *testing.T) {
		nonExistentKey := common.HexToHash("0xff")
		result, err := api.GetStoragesAt(ctx, addr, []common.Hash{key1, nonExistentKey}, blockNrOrHash)
		require.NoError(t, err)
		assert.Equal(t, val1, result[key1])
		_, exists := result[nonExistentKey]
		assert.False(t, exists)
	})

	t.Run("returns empty map for non-existent address", func(t *testing.T) {
		noAddr := common.HexToAddress("0xdead")
		result, err := api.GetStoragesAt(ctx, noAddr, []common.Hash{key1}, blockNrOrHash)
		require.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestGetRegisteredStoragesAt(t *testing.T) {
	mockCtrl, _, api, addr, key1, key2, val1, val2 := setupStorageTestBackend(t)
	defer mockCtrl.Finish()
	defer clearRegisteredStorageKeys()

	ctx := context.Background()
	blockNrOrHash := rpc.NewBlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	t.Run("returns storages for registered key set", func(t *testing.T) {
		api.RegisterStorageKeys(ctx, []common.Hash{key1, key2})
		hash := computeStorageKeyHash([]common.Hash{key1, key2})

		result, err := api.GetRegisteredStoragesAt(ctx, addr, hash, blockNrOrHash)
		require.NoError(t, err)
		assert.Equal(t, val1, result[key1])
		assert.Equal(t, val2, result[key2])
	})

	t.Run("returns error for unregistered hash", func(t *testing.T) {
		bogusHash := common.HexToHash("0xdeadbeef")
		_, err := api.GetRegisteredStoragesAt(ctx, addr, bogusHash, blockNrOrHash)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "storage keys not found")
	})
}

func TestEstimateGasWithTrace(t *testing.T) {
	chainConfig := &params.ChainConfig{}
	chainConfig.IstanbulCompatibleBlock = common.Big0
	chainConfig.LondonCompatibleBlock = common.Big0
	chainConfig.EthTxTypeCompatibleBlock = common.Big0
	chainConfig.MagmaCompatibleBlock = common.Big0
	chainConfig.KoreCompatibleBlock = common.Big0
	chainConfig.ShanghaiCompatibleBlock = common.Big0
	chainConfig.CancunCompatibleBlock = common.Big0
	chainConfig.KaiaCompatibleBlock = common.Big0
	chainConfig.PragueCompatibleBlock = common.Big0

	// codeRevertHello is a contract that always reverts with "hello"
	codeRevertHello := "0x6080604052348015600f57600080fd5b5060405162461bcd60e51b815260206004820152600560248201526468656c6c6f60d81b604482015260640160405180910390fdfe"

	var (
		account1 = common.HexToAddress("0xaaaa")
		account2 = common.HexToAddress("0xbbbb")
		account3 = common.HexToAddress("0xcccc")
		gspec    = &blockchain.Genesis{Alloc: blockchain.GenesisAlloc{
			account1: {Balance: big.NewInt(params.KAIA * 2)},
			account2: {Balance: common.Big0},
			account3: {Balance: common.Big0, Code: hexutil.MustDecode(codeRevertHello)},
		}, Config: chainConfig}

		dbm    = database.NewMemoryDBManager()
		db     = state.NewDatabase(dbm)
		block  = gspec.MustCommit(dbm)
		header = block.Header()
		chain  = &testChainContext{header: header}
	)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()
	mockBackend := mock_api.NewMockBackend(mockCtrl)
	blockchain.InitDeriveSha(chainConfig)
	api := NewKaiaBlockChainAPI(mockBackend)

	any := gomock.Any()
	getStateAndHeader := func(...interface{}) (*state.StateDB, *types.Header, error) {
		st, err := state.New(block.Root(), db, nil, nil)
		return st, header, err
	}
	getEVM := func(_ context.Context, msg blockchain.Message, st *state.StateDB, hdr *types.Header, vmConfig vm.Config) (*vm.EVM, func() error, error) {
		vmError := func() error { return nil }
		txContext := blockchain.NewEVMTxContext(msg, hdr, chainConfig)
		blockContext := blockchain.NewEVMBlockContext(hdr, chain, nil)
		return vm.NewEVM(blockContext, txContext, st, chainConfig, &vmConfig), vmError, nil
	}

	mockBackend.EXPECT().ChainConfig().Return(chainConfig).AnyTimes()
	mockBackend.EXPECT().RPCGasCap().Return(common.Big0).AnyTimes()
	mockBackend.EXPECT().RPCEVMTimeout().Return(5 * time.Second).AnyTimes()
	mockBackend.EXPECT().StateAndHeaderByNumber(any, any).DoAndReturn(getStateAndHeader).AnyTimes()
	mockBackend.EXPECT().StateAndHeaderByNumberOrHash(any, any).DoAndReturn(getStateAndHeader).AnyTimes()
	mockBackend.EXPECT().GetEVM(any, any, any, any, any).DoAndReturn(getEVM).AnyTimes()
	mockBackend.EXPECT().IsConsoleLogEnabled().Return(false).AnyTimes()

	ctx := context.Background()
	KAIA := hexutil.Big(*big.NewInt(params.KAIA))

	t.Run("successful transfer returns gas without trace", func(t *testing.T) {
		args := CallArgs{
			From:  account1,
			To:    &account2,
			Value: KAIA,
		}
		estimatedGas, estimateErr := api.EstimateGas(ctx, args, nil, nil)
		require.NoError(t, estimateErr)

		result, err := api.EstimateGasWithTrace(ctx, args)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Equal(t, estimatedGas, result.Gas)
		assert.Nil(t, result.Trace, "successful calls should not have a trace")
		assert.Nil(t, result.Error, "successful calls should not have an error")
	})

	t.Run("reverting call returns trace and error", func(t *testing.T) {
		args := CallArgs{
			From: account1,
			To:   &account3, // contract that always reverts
		}
		_, estimateErr := api.EstimateGas(ctx, args, nil, nil)
		require.Error(t, estimateErr)

		result, err := api.EstimateGasWithTrace(ctx, args)
		require.NoError(t, err) // method itself shouldn't error; the EVM error is in result.Error
		require.NotNil(t, result)

		assert.NotNil(t, result.Error, "reverting call should have an error string")
		assert.Equal(t, estimateErr.Error(), *result.Error)
		assert.NotNil(t, result.Trace, "reverting call should have a trace")
	})

	t.Run("insufficient balance returns trace and error", func(t *testing.T) {
		args := CallArgs{
			From:  account2, // 0 balance
			To:    &account1,
			Value: KAIA,
		}
		_, estimateErr := api.EstimateGas(ctx, args, nil, nil)
		require.Error(t, estimateErr)

		result, err := api.EstimateGasWithTrace(ctx, args)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.NotNil(t, result.Error)
		assert.Equal(t, estimateErr.Error(), *result.Error)
		assert.Contains(t, *result.Error, "insufficient")
	})
}
