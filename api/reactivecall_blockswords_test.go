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

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/state"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/event"
	"github.com/kaiachain/kaia/networks/rpc"
	"github.com/kaiachain/kaia/storage/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeReactiveBackend implements only the Backend methods the reactive manager
// uses: CurrentBlock and SubscribeChainHeadEvent. Everything else is nil because
// the injected evaluator replaces EVM execution.
type fakeReactiveBackend struct {
	Backend
	feed *event.Feed
	mu   sync.Mutex
	head *types.Block
}

func (f *fakeReactiveBackend) SubscribeChainHeadEvent(ch chan<- blockchain.ChainHeadEvent) event.Subscription {
	return f.feed.Subscribe(ch)
}

func (f *fakeReactiveBackend) CurrentBlock() *types.Block {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.head
}

func (f *fakeReactiveBackend) setHead(b *types.Block) {
	f.mu.Lock()
	f.head = b
	f.mu.Unlock()
}

func newReactiveTestManager(t *testing.T, eval reactiveEvaluator) (*reactiveCallManager, *fakeReactiveBackend) {
	feed := new(event.Feed)
	be := &fakeReactiveBackend{feed: feed}
	m := &reactiveCallManager{
		backend: be,
		eval:    eval,
		subs:    make(map[uint64]*reactiveCallSubscription),
	}
	t.Cleanup(m.close) // stop the head loop when the test ends
	return m, be
}

func blockWithRoot(number uint64, root common.Hash) *types.Block {
	return types.NewBlockWithHeader(&types.Header{Number: new(big.Int).SetUint64(number), Root: root})
}

// commitAccountChange commits a real StateDB that changes addr's balance,
// exercising the actual account-capture hook, and returns the resulting state
// root (distinct per salt). The address must already be on the account
// watch-list for the change to be captured.
func commitAccountChange(t *testing.T, addr common.Address, salt int64) common.Hash {
	t.Helper()
	st, err := state.New(common.Hash{}, state.NewDatabase(database.NewMemoryDBManager()), nil, nil)
	require.NoError(t, err)
	st.AddBalance(addr, big.NewInt(salt))
	root, err := st.Commit(false)
	require.NoError(t, err)
	return root
}

func recvReactive(t *testing.T, sub *reactiveCallSubscription) *CallResultNotification {
	t.Helper()
	select {
	case n := <-sub.out:
		return n
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a reactive call notification")
		return nil
	}
}

func assertNoReactive(t *testing.T, sub *reactiveCallSubscription) {
	t.Helper()
	select {
	case n := <-sub.out:
		t.Fatalf("unexpected reactive notification: %+v", n)
	case <-time.After(250 * time.Millisecond):
	}
}

func addrSet(addrs ...common.Address) map[common.Address]struct{} {
	out := make(map[common.Address]struct{}, len(addrs))
	for _, a := range addrs {
		out[a] = struct{}{}
	}
	return out
}

// staticEval returns an evaluator whose result for a block is results[blockNum]
// (default empty) and whose dependency set is fixed.
func staticEval(mu *sync.Mutex, results map[uint64]string, deps map[common.Address]struct{}, trackable bool, blockContext []string) reactiveEvaluator {
	return func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error) {
		mu.Lock()
		rd := results[blockNum]
		mu.Unlock()
		return &reactiveEvalResult{
			returnData:   []byte(rd),
			deps:         cloneAddrSet(deps),
			trackable:    trackable,
			blockContext: blockContext,
		}, nil
	}
}

// mustSubscribe subscribes and fails the test on error.
func mustSubscribe(t *testing.T, m *reactiveCallManager, args CallArgs, id rpc.ID) *reactiveCallSubscription {
	t.Helper()
	sub, err := m.subscribe(args, id)
	require.NoError(t, err)
	return sub
}

// TestReactiveCallSnapshotUpdateDedup covers the core lifecycle: initial
// snapshot, an update when the result changes, and suppression when a dependency
// changes but the result does not.
func TestReactiveCallSnapshotUpdateDedup(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	dep := common.HexToAddress("0xdead")

	var mu sync.Mutex
	results := map[uint64]string{100: "A", 101: "B", 102: "B"}
	m, be := newReactiveTestManager(t, staticEval(&mu, results, addrSet(dep), true, nil))

	be.setHead(blockWithRoot(100, common.HexToHash("0x100")))
	sub := mustSubscribe(t, m, CallArgs{}, "sub-1")
	defer m.unsubscribe(sub)

	snap := recvReactive(t, sub)
	assert.Equal(t, reactiveCallSnapshot, snap.Type)
	assert.EqualValues(t, 100, snap.BlockNumber)
	assert.Equal(t, "A", string(snap.Result))
	require.NotNil(t, snap.Trackable)
	assert.True(t, *snap.Trackable)

	// A dependency change at block 101 with a different result -> update seq 1.
	root101 := commitAccountChange(t, dep, 101)
	be.setHead(blockWithRoot(101, root101))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(101, root101)})

	upd := recvReactive(t, sub)
	assert.Equal(t, reactiveCallUpdate, upd.Type)
	assert.EqualValues(t, 1, upd.Seq)
	assert.EqualValues(t, 101, upd.BlockNumber)
	assert.Equal(t, "B", string(upd.Result))

	// A dependency change at block 102 but the result is still "B" -> suppressed.
	root102 := commitAccountChange(t, dep, 102)
	be.setHead(blockWithRoot(102, root102))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(102, root102)})

	assertNoReactive(t, sub)
}

// TestReactiveCallUntrackableWarns verifies a block-context-dependent call is
// accepted (not rejected) and the snapshot carries trackable=false + a warning.
func TestReactiveCallUntrackableWarns(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	dep := common.HexToAddress("0xbeef")

	var mu sync.Mutex
	results := map[uint64]string{50: "X"}
	m, be := newReactiveTestManager(t, staticEval(&mu, results, addrSet(dep), false, []string{"TIMESTAMP"}))

	be.setHead(blockWithRoot(50, common.HexToHash("0x50")))
	sub := mustSubscribe(t, m, CallArgs{}, "sub-2")
	defer m.unsubscribe(sub)

	snap := recvReactive(t, sub)
	assert.Equal(t, reactiveCallSnapshot, snap.Type)
	require.NotNil(t, snap.Trackable)
	assert.False(t, *snap.Trackable, "block-context call must be flagged untrackable")
	assert.Contains(t, snap.BlockContext, "TIMESTAMP")
}

// TestReactiveCallDynamicDeps verifies the engine re-arms when a re-evaluation
// reveals a new dependency, so a change to the new dependency triggers updates.
func TestReactiveCallDynamicDeps(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	depA := common.HexToAddress("0x0a")
	depB := common.HexToAddress("0x0b")

	var mu sync.Mutex
	results := map[uint64]string{10: "r10", 11: "r11", 12: "r12"}
	// deps depend on block: {A} at 10, {A,B} from 11 onward.
	eval := func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error) {
		mu.Lock()
		rd := results[blockNum]
		mu.Unlock()
		deps := addrSet(depA)
		if blockNum >= 11 {
			deps = addrSet(depA, depB)
		}
		return &reactiveEvalResult{returnData: []byte(rd), deps: deps, trackable: true}, nil
	}
	m, be := newReactiveTestManager(t, eval)

	be.setHead(blockWithRoot(10, common.HexToHash("0x10")))
	sub := mustSubscribe(t, m, CallArgs{}, "sub-3")
	defer m.unsubscribe(sub)

	snap := recvReactive(t, sub)
	assert.Equal(t, "r10", string(snap.Result))
	assert.Equal(t, addrSet(depA), sub.loadDeps(), "only A is a dependency at block 10")

	// A change to A at block 11 -> re-eval reveals B as a new dependency.
	rootA := commitAccountChange(t, depA, 11)
	be.setHead(blockWithRoot(11, rootA))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(11, rootA)})

	upd := recvReactive(t, sub)
	assert.Equal(t, "r11", string(upd.Result))
	assert.Equal(t, addrSet(depA, depB), sub.loadDeps(), "B must now be armed as a dependency")

	// A change to the newly-armed B at block 12 -> update.
	rootB := commitAccountChange(t, depB, 12)
	be.setHead(blockWithRoot(12, rootB))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(12, rootB)})

	upd2 := recvReactive(t, sub)
	assert.Equal(t, "r12", string(upd2.Result))
	assert.EqualValues(t, 2, upd2.Seq)
}

// TestReactiveCallStabilizationFallback verifies the initial dependency-growth
// loop terminates (no infinite spin) when the dependency set never stabilizes,
// falling back to self-healing and still emitting a snapshot.
func TestReactiveCallStabilizationFallback(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)

	var mu sync.Mutex
	calls := 0
	// Each evaluation reveals one brand-new dependency, so deps never stabilize.
	eval := func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		deps := make(map[common.Address]struct{}, n)
		for i := 0; i < n; i++ {
			deps[common.BigToAddress(big.NewInt(int64(i)))] = struct{}{}
		}
		return &reactiveEvalResult{returnData: []byte("z"), deps: deps, trackable: true}, nil
	}
	m, be := newReactiveTestManager(t, eval)
	be.setHead(blockWithRoot(1, common.HexToHash("0x1")))

	before := reactiveStabilizationFallbacks.Count()
	sub := mustSubscribe(t, m, CallArgs{}, "sub-4")
	defer m.unsubscribe(sub)

	snap := recvReactive(t, sub) // must still arrive (bounded loop)
	assert.Equal(t, reactiveCallSnapshot, snap.Type)
	assert.Equal(t, int64(1), reactiveStabilizationFallbacks.Count()-before, "fallback must be counted once")

	mu.Lock()
	totalCalls := calls
	mu.Unlock()
	assert.LessOrEqual(t, totalCalls, reactiveMaxStabilizationPasses+1, "evaluations must be bounded by the pass cap")
}

// TestReactiveCallList verifies the list method reports active subscriptions.
func TestReactiveCallList(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	dep := common.HexToAddress("0xcafe")

	var mu sync.Mutex
	m, be := newReactiveTestManager(t, staticEval(&mu, map[uint64]string{7: "v"}, addrSet(dep), true, nil))
	be.setHead(blockWithRoot(7, common.HexToHash("0x7")))

	sub := mustSubscribe(t, m, CallArgs{}, "sub-list")
	defer m.unsubscribe(sub)
	recvReactive(t, sub) // wait for snapshot so info is populated

	list := m.list()
	require.Len(t, list, 1)
	assert.EqualValues(t, "sub-list", list[0].ID)
	assert.Equal(t, 1, list[0].DepCount)
	assert.True(t, list[0].Trackable)
	assert.EqualValues(t, 7, list[0].LastBlock)
}

// TestReactiveCallUnsubscribeClearsWatchlist verifies teardown removes the
// subscription's dependencies from the global watch-list and stops its goroutine.
func TestReactiveCallUnsubscribeClearsWatchlist(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	dep := common.HexToAddress("0xfeed")

	var mu sync.Mutex
	m, be := newReactiveTestManager(t, staticEval(&mu, map[uint64]string{3: "q"}, addrSet(dep), true, nil))
	be.setHead(blockWithRoot(3, common.HexToHash("0x3")))

	sub := mustSubscribe(t, m, CallArgs{}, "sub-5")
	recvReactive(t, sub)

	// While active, a commit to dep is captured (dep is armed).
	require.NotEmpty(t, commitAndLookup(t, dep, 3), "dep should be watched while subscribed")

	m.unsubscribe(sub)

	// After unsubscribe, dep is no longer watched.
	assert.Empty(t, commitAndLookup(t, dep, 4), "dep must be unwatched after unsubscribe")
	assert.Error(t, sub.ctx.Err(), "subscription context must be cancelled")
}

// commitAndLookup commits a balance change to addr and returns the account
// changes captured for the resulting root.
func commitAndLookup(t *testing.T, addr common.Address, salt int64) []common.Address {
	t.Helper()
	root := commitAccountChange(t, addr, salt)
	return state.LookupBlockswordsAccountChanges(root)
}

// TestReactiveCallSeedsSenderAndRecipient verifies the engine watches the call's
// sender and recipient (read during message setup, outside the EVM) in addition
// to the EVM-observed dependencies.
func TestReactiveCallSeedsSenderAndRecipient(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	from := common.HexToAddress("0xf01")
	to := common.HexToAddress("0xf02")
	evmDep := common.HexToAddress("0xf03")

	var mu sync.Mutex
	m, be := newReactiveTestManager(t, staticEval(&mu, map[uint64]string{9: "v"}, addrSet(evmDep), true, nil))
	be.setHead(blockWithRoot(9, common.HexToHash("0x9")))

	sub := mustSubscribe(t, m, CallArgs{From: from, To: &to}, "seed")
	defer m.unsubscribe(sub)
	recvReactive(t, sub)

	deps := sub.loadDeps()
	assert.Contains(t, deps, from, "sender must be watched")
	assert.Contains(t, deps, to, "recipient must be watched")
	assert.Contains(t, deps, evmDep, "EVM-observed dependency must be watched")
}

// TestReactiveCallBackendErrorSurfacesAndRecovers verifies a terminal initial
// evaluation error is surfaced to the client (not a silent hang) and that the
// subscription recovers once evaluation succeeds.
func TestReactiveCallBackendErrorSurfacesAndRecovers(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	dep := common.HexToAddress("0xe01")

	var mu sync.Mutex
	failing := true
	eval := func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error) {
		mu.Lock()
		f := failing
		mu.Unlock()
		if f {
			return nil, errors.New("state unavailable")
		}
		return &reactiveEvalResult{returnData: []byte("ok"), deps: addrSet(dep), trackable: true}, nil
	}
	m, be := newReactiveTestManager(t, eval)
	be.setHead(blockWithRoot(1, common.HexToHash("0x1")))

	sub := mustSubscribe(t, m, CallArgs{}, "err")
	defer m.unsubscribe(sub)

	errSnap := recvReactive(t, sub)
	assert.Equal(t, reactiveCallSnapshot, errSnap.Type)
	assert.Equal(t, "state unavailable", errSnap.Error)
	assert.Empty(t, errSnap.Result)

	// Recover: subsequent evaluations succeed; a (changeless) block re-triggers
	// the not-yet-anchored subscription.
	mu.Lock()
	failing = false
	mu.Unlock()
	be.setHead(blockWithRoot(2, common.HexToHash("0x2")))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(2, common.HexToHash("0x2"))})

	okSnap := recvReactive(t, sub)
	assert.Equal(t, reactiveCallSnapshot, okSnap.Type)
	assert.Empty(t, okSnap.Error)
	assert.Equal(t, "ok", string(okSnap.Result))
}

// TestReactiveCallMonotonicDeps verifies a dependency read on one evaluation but
// not the next stays watched (the armed set is never narrowed), so a later
// change to it still triggers a re-evaluation.
func TestReactiveCallMonotonicDeps(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	depA := common.HexToAddress("0x1a")
	depB := common.HexToAddress("0x1b")

	var mu sync.Mutex
	results := map[uint64]string{20: "r20", 21: "r21"}
	// {A,B} at block 20, then only {A} at block 21 (B conditionally dropped).
	eval := func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error) {
		mu.Lock()
		rd := results[blockNum]
		mu.Unlock()
		deps := addrSet(depA, depB)
		if blockNum >= 21 {
			deps = addrSet(depA)
		}
		return &reactiveEvalResult{returnData: []byte(rd), deps: deps, trackable: true}, nil
	}
	m, be := newReactiveTestManager(t, eval)
	be.setHead(blockWithRoot(20, common.HexToHash("0x20")))

	sub := mustSubscribe(t, m, CallArgs{}, "mono")
	defer m.unsubscribe(sub)
	recvReactive(t, sub)
	require.Equal(t, addrSet(depA, depB), sub.loadDeps())

	// A change to A at block 21 makes the eval drop B from its result set.
	rootA := commitAccountChange(t, depA, 21)
	be.setHead(blockWithRoot(21, rootA))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(21, rootA)})
	recvReactive(t, sub)

	assert.Contains(t, sub.loadDeps(), depB, "conditionally-dropped dependency must stay watched")
}
