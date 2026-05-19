package cn

import (
	"context"
	"math/big"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/state"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/crypto"
	"github.com/kaiachain/kaia/networks/rpc"
	mocks2 "github.com/kaiachain/kaia/node/cn/mocks"
	"github.com/kaiachain/kaia/params"
	"github.com/kaiachain/kaia/storage/database"
	"github.com/kaiachain/kaia/work/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dumpContractStorageWithPersistent reproduces the old (broken) code path that
// used StateAtWithPersistent instead of StateAt.
func dumpContractStorageWithPersistent(api *DebugCNAPI, ctx context.Context, addresses []common.Address, blockNumberOrHash rpc.BlockNumberOrHash) (map[string]map[string]string, error) {
	block, err := api.cn.APIBackend.BlockByNumberOrHash(ctx, blockNumberOrHash)
	if err != nil {
		return nil, err
	}

	stateDB, err := api.cn.BlockChain().StateAtWithPersistent(block.Root())
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

func TestDebugCNAPI_DumpContractStorage(t *testing.T) {
	blockchain.InitDeriveSha(params.TestChainConfig)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBlockChain := mocks.NewMockBlockChain(mockCtrl)
	mockMiner := mocks2.NewMockMiner(mockCtrl)

	cn := &CN{blockchain: mockBlockChain, miner: mockMiner}
	cn.APIBackend = &CNAPIBackend{cn: cn}
	debugAPI := NewDebugCNAPI(cn)

	// Create a state with known storage values
	memDBManager := database.NewMemoryDBManager()
	stateDatabase := state.NewDatabase(memDBManager)
	stateDB, _ := state.New(common.Hash{}, stateDatabase, nil, nil)

	addr1 := common.HexToAddress("0x1111")
	addr2 := common.HexToAddress("0x2222")

	key1 := common.HexToHash("0x01")
	key2 := common.HexToHash("0x02")
	val1 := common.HexToHash("0xaa")
	val2 := common.HexToHash("0xbb")

	stateDB.AddBalance(addr1, big.NewInt(1))
	stateDB.SetState(addr1, key1, val1)
	stateDB.SetState(addr1, key2, val2)

	stateDB.AddBalance(addr2, big.NewInt(1))
	stateDB.SetState(addr2, key1, common.HexToHash("0xcc"))

	root, err := stateDB.Commit(false)
	require.NoError(t, err)
	stateDatabase.TrieDB().Commit(root, false, 0)

	// Use a non-snapshot block number (not divisible by 128) to simulate the
	// real-world scenario where StateAtWithPersistent fails.
	nonSnapshotBlockNum := big.NewInt(129)
	header := &types.Header{
		Number: nonSnapshotBlockNum,
		Root:   root,
	}
	block := types.NewBlock(header, nil, nil)

	latestBlockNr := rpc.LatestBlockNumber
	blockNrOrHash := rpc.BlockNumberOrHash{BlockNumber: &latestBlockNr}

	// Regression: the old code used StateAtWithPersistent which rejects
	// non-snapshot blocks with ErrNotExistNode because their trie nodes
	// are not persisted to disk (only kept in memory cache).
	t.Run("old code fails on non-snapshot block with StateAtWithPersistent", func(t *testing.T) {
		mockBlockChain.EXPECT().CurrentBlock().Return(block).Times(1)
		// StateAtWithPersistent returns ErrNotExistNode for non-snapshot blocks
		// because the root node is not found in persistent storage.
		mockBlockChain.EXPECT().StateAtWithPersistent(root).Return(nil, blockchain.ErrNotExistNode).Times(1)

		result, err := dumpContractStorageWithPersistent(debugAPI, context.Background(), []common.Address{addr1, addr2}, blockNrOrHash)
		require.ErrorIs(t, err, blockchain.ErrNotExistNode)
		require.Nil(t, result)
	})

	// Fix: the new code uses StateAt which does not check persistent storage,
	// allowing it to use trie nodes from the in-memory cache.
	t.Run("new code succeeds on non-snapshot block with StateAt", func(t *testing.T) {
		mockBlockChain.EXPECT().CurrentBlock().Return(block).Times(1)
		mockBlockChain.EXPECT().StateAt(root).DoAndReturn(func(r common.Hash) (*state.StateDB, error) {
			return state.New(r, stateDatabase, nil, nil)
		}).Times(1)

		result, err := debugAPI.DumpContractStorage(context.Background(), []common.Address{addr1, addr2}, blockNrOrHash)
		require.NoError(t, err)
		require.NotNil(t, result)

		assert.Contains(t, result, addr1.String())
		assert.Contains(t, result, addr2.String())
		assert.Len(t, result[addr1.String()], 2)
		assert.Len(t, result[addr2.String()], 1)
	})

	t.Run("hashed dump returns hashed trie keys", func(t *testing.T) {
		mockBlockChain.EXPECT().CurrentBlock().Return(block).Times(1)
		mockBlockChain.EXPECT().StateAt(root).DoAndReturn(func(r common.Hash) (*state.StateDB, error) {
			return state.New(r, stateDatabase, nil, nil)
		}).Times(1)

		result, err := debugAPI.DumpContractStorageHash(context.Background(), []common.Address{addr1, addr2}, blockNrOrHash)
		require.NoError(t, err)
		require.NotNil(t, result)

		addr1Storage := result[addr1.String()]
		require.Len(t, addr1Storage, 2)

		hashedKey1 := "hash:" + common.Bytes2Hex(crypto.Keccak256(key1.Bytes()))
		hashedKey2 := "hash:" + common.Bytes2Hex(crypto.Keccak256(key2.Bytes()))
		assert.Contains(t, addr1Storage, hashedKey1)
		assert.Contains(t, addr1Storage, hashedKey2)
		for key := range addr1Storage {
			assert.Regexp(t, "^hash:", key)
		}
	})

	t.Run("storage dump respects canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, err := debugAPI.DumpContractStorage(ctx, []common.Address{addr1}, blockNrOrHash)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
	})

	t.Run("hashed storage dump respects deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()

		result, err := debugAPI.DumpContractStorageHash(ctx, []common.Address{addr1}, blockNrOrHash)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Nil(t, result)
	})
}

func TestDebugCNAPI_DiffContractStorageHash(t *testing.T) {
	blockchain.InitDeriveSha(params.TestChainConfig)

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockBlockChain := mocks.NewMockBlockChain(mockCtrl)
	mockMiner := mocks2.NewMockMiner(mockCtrl)

	cn := &CN{blockchain: mockBlockChain, miner: mockMiner}
	cn.APIBackend = &CNAPIBackend{cn: cn}
	debugAPI := NewDebugCNAPI(cn)

	stateDatabase := state.NewDatabase(database.NewMemoryDBManager())
	addr1 := common.HexToAddress("0x1111")
	addr2 := common.HexToAddress("0x2222")
	keySame := common.HexToHash("0x01")
	keyUpdate := common.HexToHash("0x02")
	keyDelete := common.HexToHash("0x03")
	keyAdd := common.HexToHash("0x04")
	keyAddr2 := common.HexToHash("0x05")

	root0 := commitCNTestState(t, stateDatabase, common.Hash{}, func(s *state.StateDB) {
		s.AddBalance(addr1, big.NewInt(1))
		s.SetState(addr1, keySame, common.HexToHash("0xaa"))
		s.SetState(addr1, keyUpdate, common.HexToHash("0xbb"))
		s.SetState(addr1, keyDelete, common.HexToHash("0xcc"))
		s.AddBalance(addr2, big.NewInt(1))
		s.SetState(addr2, keyAddr2, common.HexToHash("0x11"))
	})
	root1 := commitCNTestState(t, stateDatabase, root0, func(s *state.StateDB) {
		s.SetState(addr1, keySame, common.HexToHash("0xaa"))
		s.SetState(addr1, keyUpdate, common.HexToHash("0xdd"))
		s.SetState(addr1, keyDelete, common.Hash{})
		s.SetState(addr1, keyAdd, common.HexToHash("0xee"))
		s.SetState(addr2, keyAddr2, common.HexToHash("0x22"))
	})

	blockFrom := types.NewBlock(&types.Header{Number: big.NewInt(10), Root: root0}, nil, nil)
	blockTo := types.NewBlock(&types.Header{Number: big.NewInt(11), Root: root1}, nil, nil)
	fromBlock := rpc.NewBlockNumberOrHashWithNumber(rpc.BlockNumber(10))
	toBlock := rpc.NewBlockNumberOrHashWithNumber(rpc.BlockNumber(11))

	openState := func(root common.Hash) (*state.StateDB, error) {
		return state.New(root, stateDatabase, nil, nil)
	}
	hashedKey := func(key common.Hash) string {
		return "hash:" + common.Bytes2Hex(crypto.Keccak256(key.Bytes()))
	}

	t.Run("returns explicit block diff for multiple addresses", func(t *testing.T) {
		mockBlockChain.EXPECT().GetBlockByNumber(uint64(10)).Return(blockFrom).Times(1)
		mockBlockChain.EXPECT().GetBlockByNumber(uint64(11)).Return(blockTo).Times(1)
		mockBlockChain.EXPECT().StateAt(root0).DoAndReturn(openState).Times(1)
		mockBlockChain.EXPECT().StateAt(root1).DoAndReturn(openState).Times(1)

		result, err := debugAPI.DiffContractStorageHash(context.Background(), []common.Address{addr1, addr2}, fromBlock, &toBlock, nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, hexutil.Uint64(10), result.FromBlock)
		assert.Equal(t, hexutil.Uint64(11), result.ToBlock)

		addr1Storage := result.Storage[addr1.String()]
		require.Len(t, addr1Storage, 2)
		assert.Contains(t, addr1Storage, hashedKey(keyUpdate))
		assert.Contains(t, addr1Storage, hashedKey(keyAdd))
		assert.NotContains(t, addr1Storage, hashedKey(keySame))
		assert.NotContains(t, addr1Storage, hashedKey(keyDelete))
		assert.NotNil(t, addr1Storage[hashedKey(keyUpdate)])
		assert.NotNil(t, addr1Storage[hashedKey(keyAdd)])

		addr2Storage := result.Storage[addr2.String()]
		require.Len(t, addr2Storage, 1)
		assert.Contains(t, addr2Storage, hashedKey(keyAddr2))
		assert.NotNil(t, addr2Storage[hashedKey(keyAddr2)])
	})

	t.Run("omitted toBlock uses latest and includes deletions when requested", func(t *testing.T) {
		includeDeletions := true
		mockBlockChain.EXPECT().GetBlockByNumber(uint64(10)).Return(blockFrom).Times(1)
		mockBlockChain.EXPECT().CurrentBlock().Return(blockTo).Times(1)
		mockBlockChain.EXPECT().StateAt(root0).DoAndReturn(openState).Times(1)
		mockBlockChain.EXPECT().StateAt(root1).DoAndReturn(openState).Times(1)

		result, err := debugAPI.DiffContractStorageHash(context.Background(), []common.Address{addr1}, fromBlock, nil, &includeDeletions)
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.Equal(t, hexutil.Uint64(10), result.FromBlock)
		assert.Equal(t, hexutil.Uint64(11), result.ToBlock)

		addr1Storage := result.Storage[addr1.String()]
		require.Len(t, addr1Storage, 3)
		assert.NotNil(t, addr1Storage[hashedKey(keyUpdate)])
		assert.NotNil(t, addr1Storage[hashedKey(keyAdd)])
		assert.Contains(t, addr1Storage, hashedKey(keyDelete))
		assert.Nil(t, addr1Storage[hashedKey(keyDelete)])
	})

	t.Run("rejects invalid block order", func(t *testing.T) {
		mockBlockChain.EXPECT().GetBlockByNumber(uint64(11)).Return(blockTo).Times(1)
		mockBlockChain.EXPECT().GetBlockByNumber(uint64(10)).Return(blockFrom).Times(1)

		_, err := debugAPI.DiffContractStorageHash(context.Background(), []common.Address{addr1}, toBlock, &fromBlock, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "from block height")
	})

	t.Run("respects canceled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, err := debugAPI.DiffContractStorageHash(ctx, []common.Address{addr1}, fromBlock, &toBlock, nil)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, result)
	})
}

func commitCNTestState(t *testing.T, db state.Database, root common.Hash, mutate func(*state.StateDB)) common.Hash {
	t.Helper()

	stateDB, err := state.New(root, db, nil, nil)
	require.NoError(t, err)
	mutate(stateDB)

	newRoot, err := stateDB.Commit(false)
	require.NoError(t, err)
	require.NoError(t, db.TrieDB().Commit(newRoot, false, 0))
	return newRoot
}
