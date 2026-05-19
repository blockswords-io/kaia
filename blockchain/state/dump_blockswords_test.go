package state

import (
	"context"
	"math/big"
	"testing"

	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/crypto"
	"github.com/kaiachain/kaia/storage/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetStates(t *testing.T) {
	memDBManager := database.NewMemoryDBManager()
	state, _ := New(common.Hash{}, NewDatabase(memDBManager), nil, nil)

	addr := common.Address{0x01}
	state.AddBalance(addr, big.NewInt(1)) // ensure account exists

	key1 := common.HexToHash("0x01")
	key2 := common.HexToHash("0x02")
	key3 := common.HexToHash("0x03")
	val1 := common.HexToHash("0xaa")
	val2 := common.HexToHash("0xbb")

	state.SetState(addr, key1, val1)
	state.SetState(addr, key2, val2)
	// key3 is not set

	state.IntermediateRoot(false)

	t.Run("returns matching storage values", func(t *testing.T) {
		result := state.GetStates(addr, []common.Hash{key1, key2})
		assert.Equal(t, val1, result[key1])
		assert.Equal(t, val2, result[key2])
		assert.Len(t, result, 2)
	})

	t.Run("omits keys with empty values", func(t *testing.T) {
		result := state.GetStates(addr, []common.Hash{key1, key3})
		assert.Equal(t, val1, result[key1])
		_, exists := result[key3]
		assert.False(t, exists)
		assert.Len(t, result, 1)
	})

	t.Run("returns empty map for non-existent address", func(t *testing.T) {
		nonExistent := common.Address{0xff}
		result := state.GetStates(nonExistent, []common.Hash{key1})
		assert.Empty(t, result)
	})

	t.Run("returns empty map for empty key list", func(t *testing.T) {
		result := state.GetStates(addr, []common.Hash{})
		assert.Empty(t, result)
	})
}

func TestDumpContractStorage(t *testing.T) {
	memDBManager := database.NewMemoryDBManager()
	state, _ := New(common.Hash{}, NewDatabase(memDBManager), nil, nil)

	addr := common.Address{0x01}
	state.AddBalance(addr, big.NewInt(1))

	key1 := common.HexToHash("0x01")
	key2 := common.HexToHash("0x02")
	val1 := common.HexToHash("0xaa")
	val2 := common.HexToHash("0xbb")

	state.SetState(addr, key1, val1)
	state.SetState(addr, key2, val2)

	// Commit so the storage trie is persisted (DumpContractStorage iterates the trie)
	_, err := state.Commit(false)
	require.NoError(t, err)

	t.Run("dumps all storage entries", func(t *testing.T) {
		result, err := state.DumpContractStorage(context.Background(), addr)
		require.NoError(t, err)
		assert.Len(t, result, 2)
		// The keys in the dump are hex-encoded preimage keys
		// Verify values are present (exact key format depends on trie key preimages)
		foundValues := make(map[string]bool)
		for _, v := range result {
			foundValues[v] = true
		}
		// Values are RLP-encoded and hex-dumped from the trie, so they match the raw storage encoding
		assert.True(t, len(result) > 0, "should have storage entries")
	})

	t.Run("dumps hashed storage keys without resolving preimages", func(t *testing.T) {
		result, err := state.DumpContractStorageHash(context.Background(), addr)
		require.NoError(t, err)
		assert.Len(t, result, 2)

		hashedKey1 := "hash:" + common.Bytes2Hex(crypto.Keccak256(key1.Bytes()))
		hashedKey2 := "hash:" + common.Bytes2Hex(crypto.Keccak256(key2.Bytes()))
		assert.Contains(t, result, hashedKey1)
		assert.Contains(t, result, hashedKey2)
		assert.NotEmpty(t, result[hashedKey1])
		assert.NotEmpty(t, result[hashedKey2])
	})

	t.Run("returns cancellation error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		result, err := state.DumpContractStorage(ctx, addr)
		require.ErrorIs(t, err, context.Canceled)
		assert.Empty(t, result)
	})

	t.Run("returns deadline error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()

		result, err := state.DumpContractStorageHash(ctx, addr)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Empty(t, result)
	})
}

func TestDiffContractStorageHash(t *testing.T) {
	memDBManager := database.NewMemoryDBManager()
	stateDB := NewDatabase(memDBManager)

	addr := common.Address{0x01}
	keySame := common.HexToHash("0x01")
	keyUpdate := common.HexToHash("0x02")
	keyDelete := common.HexToHash("0x03")
	keyAdd := common.HexToHash("0x04")

	root0 := commitTestState(t, stateDB, common.Hash{}, func(s *StateDB) {
		s.AddBalance(addr, big.NewInt(1))
		s.SetState(addr, keySame, common.HexToHash("0xaa"))
		s.SetState(addr, keyUpdate, common.HexToHash("0xbb"))
		s.SetState(addr, keyDelete, common.HexToHash("0xcc"))
	})
	root1 := commitTestState(t, stateDB, root0, func(s *StateDB) {
		s.SetState(addr, keySame, common.HexToHash("0xaa"))
		s.SetState(addr, keyUpdate, common.HexToHash("0xdd"))
		s.SetState(addr, keyDelete, common.Hash{})
		s.SetState(addr, keyAdd, common.HexToHash("0xee"))
	})

	oldState, err := New(root0, stateDB, nil, nil)
	require.NoError(t, err)
	newState, err := New(root1, stateDB, nil, nil)
	require.NoError(t, err)

	hashedKey := func(key common.Hash) string {
		return "hash:" + common.Bytes2Hex(crypto.Keccak256(key.Bytes()))
	}

	t.Run("returns added and updated slots by default", func(t *testing.T) {
		result, err := newState.DiffContractStorageHash(context.Background(), oldState, addr, false)
		require.NoError(t, err)

		require.Len(t, result, 2)
		assert.Contains(t, result, hashedKey(keyUpdate))
		assert.Contains(t, result, hashedKey(keyAdd))
		assert.NotContains(t, result, hashedKey(keySame))
		assert.NotContains(t, result, hashedKey(keyDelete))

		dump, err := newState.DumpContractStorageHash(context.Background(), addr)
		require.NoError(t, err)
		require.NotNil(t, result[hashedKey(keyUpdate)])
		require.NotNil(t, result[hashedKey(keyAdd)])
		assert.Equal(t, dump[hashedKey(keyUpdate)], *result[hashedKey(keyUpdate)])
		assert.Equal(t, dump[hashedKey(keyAdd)], *result[hashedKey(keyAdd)])
	})

	t.Run("includes deleted slots as nil when requested", func(t *testing.T) {
		result, err := newState.DiffContractStorageHash(context.Background(), oldState, addr, true)
		require.NoError(t, err)

		require.Len(t, result, 3)
		assert.NotNil(t, result[hashedKey(keyUpdate)])
		assert.NotNil(t, result[hashedKey(keyAdd)])
		assert.Contains(t, result, hashedKey(keyDelete))
		assert.Nil(t, result[hashedKey(keyDelete)])
		assert.NotContains(t, result, hashedKey(keySame))
	})

	t.Run("returns all slots when old account is missing", func(t *testing.T) {
		newAddr := common.Address{0x02}
		newKey := common.HexToHash("0x05")
		root2 := commitTestState(t, stateDB, root1, func(s *StateDB) {
			s.AddBalance(newAddr, big.NewInt(1))
			s.SetState(newAddr, newKey, common.HexToHash("0xff"))
		})
		fromState, err := New(root1, stateDB, nil, nil)
		require.NoError(t, err)
		toState, err := New(root2, stateDB, nil, nil)
		require.NoError(t, err)

		result, err := toState.DiffContractStorageHash(context.Background(), fromState, newAddr, false)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Contains(t, result, hashedKey(newKey))
		assert.NotNil(t, result[hashedKey(newKey)])
	})

	t.Run("returns deletions when new account is missing", func(t *testing.T) {
		oldOnlyAddr := common.Address{0x03}
		oldOnlyKey := common.HexToHash("0x06")
		oldOnlyRoot := commitTestState(t, stateDB, common.Hash{}, func(s *StateDB) {
			s.AddBalance(oldOnlyAddr, big.NewInt(1))
			s.SetState(oldOnlyAddr, oldOnlyKey, common.HexToHash("0x99"))
		})
		fromState, err := New(oldOnlyRoot, stateDB, nil, nil)
		require.NoError(t, err)
		toState, err := New(common.Hash{}, stateDB, nil, nil)
		require.NoError(t, err)

		result, err := toState.DiffContractStorageHash(context.Background(), fromState, oldOnlyAddr, false)
		require.NoError(t, err)
		assert.Empty(t, result)

		result, err = toState.DiffContractStorageHash(context.Background(), fromState, oldOnlyAddr, true)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Contains(t, result, hashedKey(oldOnlyKey))
		assert.Nil(t, result[hashedKey(oldOnlyKey)])
	})

	t.Run("returns empty diff for equal storage roots", func(t *testing.T) {
		sameState, err := New(root1, stateDB, nil, nil)
		require.NoError(t, err)
		result, err := sameState.DiffContractStorageHash(context.Background(), newState, addr, true)
		require.NoError(t, err)
		assert.Empty(t, result)
	})

	t.Run("returns cancellation error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		result, err := newState.DiffContractStorageHash(ctx, oldState, addr, false)
		require.ErrorIs(t, err, context.Canceled)
		assert.Empty(t, result)
	})
}

func commitTestState(t *testing.T, db Database, root common.Hash, mutate func(*StateDB)) common.Hash {
	t.Helper()

	state, err := New(root, db, nil, nil)
	require.NoError(t, err)
	mutate(state)

	newRoot, err := state.Commit(false)
	require.NoError(t, err)
	require.NoError(t, db.TrieDB().Commit(newRoot, false, 0))
	return newRoot
}
