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
	return m.subscribe(args, id)
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

// TestReactiveCallBlockContextReevaluatesEveryBlock verifies a block-context
// call (Trackable=false) is re-evaluated on EVERY block — even one with no
// watched dependency change — so a result that drifts with block context (e.g. a
// TIMESTAMP-derived value) still emits updates, and is deduped when unchanged.
// This is the completeness guarantee: callResults emits whenever the result
// actually changes, never only on state changes.
func TestReactiveCallBlockContextReevaluatesEveryBlock(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	dep := common.HexToAddress("0xb10c")

	var mu sync.Mutex
	// The result drifts at block 61, then holds at 62. No account changes are
	// committed for these blocks (their roots have no captured changes), so only
	// the block-context path — not a dependency change — can trigger re-eval.
	results := map[uint64]string{60: "t60", 61: "t61", 62: "t61"}
	m, be := newReactiveTestManager(t, staticEval(&mu, results, addrSet(dep), false, []string{"TIMESTAMP"}))

	be.setHead(blockWithRoot(60, common.HexToHash("0x60")))
	sub := mustSubscribe(t, m, CallArgs{}, "bctx")
	defer m.unsubscribe(sub)

	snap := recvReactive(t, sub)
	assert.Equal(t, reactiveCallSnapshot, snap.Type)
	assert.Equal(t, "t60", string(snap.Result))
	require.NotNil(t, snap.Trackable)
	assert.False(t, *snap.Trackable)

	// A changeless block (no account change captured for its root) still
	// re-evaluates the block-context call, so the drifted result is emitted.
	be.setHead(blockWithRoot(61, common.HexToHash("0x61")))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(61, common.HexToHash("0x61"))})

	upd := recvReactive(t, sub)
	assert.Equal(t, reactiveCallUpdate, upd.Type)
	assert.EqualValues(t, 1, upd.Seq)
	assert.EqualValues(t, 61, upd.BlockNumber)
	assert.Equal(t, "t61", string(upd.Result))

	// Another changeless block where the result holds -> re-evaluated but deduped.
	be.setHead(blockWithRoot(62, common.HexToHash("0x62")))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(62, common.HexToHash("0x62"))})

	assertNoReactive(t, sub)
}

// TestReactiveCallPublishRedeliversDroppedUpdate verifies that when the delivery
// buffer is full, publish does NOT advance the dedup state (lastHash/seq) — so a
// dropped update is re-pushed rather than silently skipped once the consumer
// drains, and the subscription is marked needsReeval so the head loop keeps
// re-attempting it.
func TestReactiveCallPublishRedeliversDroppedUpdate(t *testing.T) {
	sub := &reactiveCallSubscription{out: make(chan *CallResultNotification, 1)}
	sub.anchored.Store(true)
	sub.lastHash = reactiveResultHash([]byte("A"), "") // last delivered result was "A"

	head := blockWithRoot(10, common.HexToHash("0x10"))
	resB := &reactiveEvalResult{returnData: []byte("B"), trackable: true}

	// Fill the buffer so the next emit drops.
	sub.out <- &CallResultNotification{}

	sub.publish(head, resB)
	assert.True(t, sub.needsReeval.Load(), "a dropped update must arm needsReeval")
	assert.Equal(t, reactiveResultHash([]byte("A"), ""), sub.lastHash, "lastHash must NOT advance on a dropped update")
	assert.EqualValues(t, 0, sub.seq, "seq must NOT advance on a dropped update")

	// Drain; re-publishing the still-current result now delivers it, advances the
	// dedup state, and clears needsReeval.
	<-sub.out
	sub.publish(head, resB)
	upd := <-sub.out
	assert.Equal(t, reactiveCallUpdate, upd.Type)
	assert.Equal(t, "B", string(upd.Result))
	assert.EqualValues(t, 1, upd.Seq)
	assert.False(t, sub.needsReeval.Load(), "needsReeval must clear once delivered")
}

// TestReactiveCallPublishDroppedSnapshotStaysUnanchored verifies that when the
// initial snapshot send is dropped (consumer buffer full), publish does NOT anchor
// the subscription — so the head loop keeps re-attempting it (via !anchored) until
// the snapshot lands, rather than advancing to updates the client never got a
// baseline for.
func TestReactiveCallPublishDroppedSnapshotStaysUnanchored(t *testing.T) {
	sub := &reactiveCallSubscription{out: make(chan *CallResultNotification, 1)}
	head := blockWithRoot(7, common.HexToHash("0x7"))
	resA := &reactiveEvalResult{returnData: []byte("A"), trackable: true}

	// Fill the buffer so the snapshot send drops.
	sub.out <- &CallResultNotification{}

	sub.publish(head, resA)
	assert.False(t, sub.anchored.Load(), "a dropped snapshot must NOT anchor")
	assert.Equal(t, common.Hash{}, sub.lastHash, "lastHash must NOT advance on a dropped snapshot")

	// Drain; re-publishing now delivers the snapshot and anchors.
	<-sub.out
	sub.publish(head, resA)
	snap := <-sub.out
	assert.Equal(t, reactiveCallSnapshot, snap.Type)
	assert.Equal(t, "A", string(snap.Result))
	assert.True(t, sub.anchored.Load(), "snapshot delivery must anchor")
}

// TestReactiveCallDropCoalescesToLatestOutcome verifies that when a send is dropped
// (needsReeval armed, dedup state not advanced) and the outcome then CHANGES (here
// success -> revert), the engine converges to the LATEST outcome once the consumer
// drains — the intermediate dropped value is not redelivered, the current one is.
func TestReactiveCallDropCoalescesToLatestOutcome(t *testing.T) {
	sub := &reactiveCallSubscription{out: make(chan *CallResultNotification, 1)}
	sub.anchored.Store(true)
	sub.lastHash = reactiveResultHash([]byte("A"), "") // last delivered outcome

	head := blockWithRoot(10, common.HexToHash("0x10"))

	// Fill the buffer; publishing "B" drops (needsReeval armed, dedup state held).
	sub.out <- &CallResultNotification{}
	sub.publish(head, &reactiveEvalResult{returnData: []byte("B"), trackable: true})
	assert.True(t, sub.needsReeval.Load(), "a dropped update arms needsReeval")
	assert.Equal(t, reactiveResultHash([]byte("A"), ""), sub.lastHash, "dedup state must not advance on a drop")

	// The outcome changes to a revert before the consumer drains. Drain, then publish
	// the CURRENT outcome (the revert): it is delivered as the update, superseding
	// the dropped "B" (which is never redelivered).
	<-sub.out
	sub.publish(head, &reactiveEvalResult{vmErr: "execution reverted", trackable: true})
	upd := <-sub.out
	assert.Equal(t, reactiveCallUpdate, upd.Type)
	assert.Empty(t, upd.Result)
	assert.Equal(t, "execution reverted", upd.Error)
	assert.EqualValues(t, 1, upd.Seq)
	assert.False(t, sub.needsReeval.Load(), "needsReeval clears once the latest outcome is delivered")
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

// TestReactiveCallUnsubscribeStopsEvaluation verifies the per-subscription eval
// goroutine stops doing work after teardown: no further evaluations occur even when
// new head events and dependency changes arrive. This is the anti-leak property for
// the engine's heaviest per-subscription resource (the per-block EVM re-evaluation
// goroutine) — on a disconnect/hang the goroutine and its watch-list entries are
// released, not left re-evaluating forever.
func TestReactiveCallUnsubscribeStopsEvaluation(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	dep := common.HexToAddress("0xc0ffee")

	var mu sync.Mutex
	evalCount := 0
	eval := func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error) {
		mu.Lock()
		evalCount++
		mu.Unlock()
		return &reactiveEvalResult{returnData: []byte("v"), deps: addrSet(dep), trackable: true}, nil
	}
	m, be := newReactiveTestManager(t, eval)
	be.setHead(blockWithRoot(1, common.HexToHash("0x1")))

	sub := mustSubscribe(t, m, CallArgs{}, "stop")
	recvReactive(t, sub) // snapshot

	m.unsubscribe(sub)
	require.Error(t, sub.ctx.Err(), "ctx must be cancelled")
	mu.Lock()
	countAtUnsub := evalCount
	mu.Unlock()

	// Drive several blocks with dependency changes; a live eval goroutine would
	// re-evaluate (the dep is/was watched). A torn-down one must not.
	for i := int64(2); i < 6; i++ {
		root := commitAccountChange(t, dep, i)
		be.setHead(blockWithRoot(uint64(i), root))
		be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(uint64(i), root)})
	}
	time.Sleep(300 * time.Millisecond) // give any erroneous re-evaluation time to run

	mu.Lock()
	finalCount := evalCount
	mu.Unlock()
	assert.Equal(t, countAtUnsub, finalCount, "no evaluation may occur after unsubscribe")
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
	assert.Equal(t, reactiveCallError, errSnap.Type)
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

// TestReactiveCallEmitsOnRevertTransitions verifies a revert is a first-class
// outcome: a success<->revert transition is emitted (so a client can react to a
// call that starts or stops failing), while a stable outcome (the same value, or
// still reverting with the same error) is deduped.
func TestReactiveCallEmitsOnRevertTransitions(t *testing.T) {
	sub := &reactiveCallSubscription{out: make(chan *CallResultNotification, 8)}
	head := blockWithRoot(5, common.HexToHash("0x5"))
	success := func(v string) *reactiveEvalResult {
		return &reactiveEvalResult{returnData: []byte(v), trackable: true}
	}
	revert := func() *reactiveEvalResult {
		return &reactiveEvalResult{vmErr: "execution reverted", trackable: true}
	}
	drain := func() *CallResultNotification {
		select {
		case n := <-sub.out:
			return n
		default:
			return nil
		}
	}

	// Snapshot: success "A".
	sub.publish(head, success("A"))
	snap := drain()
	require.NotNil(t, snap)
	assert.Equal(t, reactiveCallSnapshot, snap.Type)
	assert.Equal(t, "A", string(snap.Result))
	assert.Empty(t, snap.Error)

	// success -> revert: emitted as an update (Error set, empty Result).
	sub.publish(head, revert())
	upd := drain()
	require.NotNil(t, upd)
	assert.Equal(t, reactiveCallUpdate, upd.Type)
	assert.EqualValues(t, 1, upd.Seq)
	assert.Empty(t, upd.Result)
	assert.Equal(t, "execution reverted", upd.Error)

	// Still reverting (same error) -> deduped.
	sub.publish(head, revert())
	assert.Nil(t, drain(), "a stable revert must be deduped")

	// revert -> success "A": emitted (the call recovered).
	sub.publish(head, success("A"))
	rec := drain()
	require.NotNil(t, rec)
	assert.Equal(t, reactiveCallUpdate, rec.Type)
	assert.EqualValues(t, 2, rec.Seq)
	assert.Equal(t, "A", string(rec.Result))
	assert.Empty(t, rec.Error)

	// Same success value -> deduped.
	sub.publish(head, success("A"))
	assert.Nil(t, drain(), "an unchanged value must be deduped")
}

// TestReactiveCallInitialRevertAnchorsAsSnapshot verifies that when the FIRST
// evaluated outcome is a revert, it anchors as the snapshot (with Error set) — a
// revert is a valid current outcome — and a later success is emitted as an update.
func TestReactiveCallInitialRevertAnchorsAsSnapshot(t *testing.T) {
	sub := &reactiveCallSubscription{out: make(chan *CallResultNotification, 4)}
	head := blockWithRoot(5, common.HexToHash("0x5"))
	drain := func() *CallResultNotification {
		select {
		case n := <-sub.out:
			return n
		default:
			return nil
		}
	}

	// First outcome is a revert: snapshot with Error, anchored.
	sub.publish(head, &reactiveEvalResult{vmErr: "execution reverted", trackable: true})
	snap := drain()
	require.NotNil(t, snap)
	assert.Equal(t, reactiveCallSnapshot, snap.Type)
	assert.Equal(t, "execution reverted", snap.Error)
	assert.Empty(t, snap.Result)
	assert.True(t, sub.anchored.Load(), "a revert outcome anchors the subscription")

	// First success -> update.
	sub.publish(head, &reactiveEvalResult{returnData: []byte("ok"), trackable: true})
	upd := drain()
	require.NotNil(t, upd)
	assert.Equal(t, reactiveCallUpdate, upd.Type)
	assert.EqualValues(t, 1, upd.Seq)
	assert.Equal(t, "ok", string(upd.Result))
	assert.Empty(t, upd.Error)
}

// TestReactiveCallInfraErrorThenRevertAnchors verifies the infra-error vs revert
// boundary: a backend evaluation error (the node could not run the call) surfaces
// the one-time "error" message and does NOT anchor; and if the first EVALUABLE
// outcome is then a revert, that revert anchors as the snapshot (errorReported
// gates only the "error" message, never the snapshot).
func TestReactiveCallInfraErrorThenRevertAnchors(t *testing.T) {
	sub := &reactiveCallSubscription{out: make(chan *CallResultNotification, 4)}
	sub.ctx, sub.cancel = context.WithCancel(context.Background())
	defer sub.cancel()
	head := blockWithRoot(5, common.HexToHash("0x5"))
	drain := func() *CallResultNotification {
		select {
		case n := <-sub.out:
			return n
		default:
			return nil
		}
	}

	// Backend eval error: one-time "error" message, not anchored.
	sub.handleEvalError(head, errors.New("state unavailable"))
	first := drain()
	require.NotNil(t, first)
	assert.Equal(t, reactiveCallError, first.Type)
	assert.Equal(t, "state unavailable", first.Error)
	assert.False(t, sub.anchored.Load(), "an infra error must not anchor")

	// The first evaluable outcome is a revert: it anchors as the snapshot.
	sub.publish(head, &reactiveEvalResult{vmErr: "execution reverted", trackable: true})
	snap := drain()
	require.NotNil(t, snap)
	assert.Equal(t, reactiveCallSnapshot, snap.Type)
	assert.Equal(t, "execution reverted", snap.Error)
	assert.Empty(t, snap.Result)
	assert.True(t, sub.anchored.Load(), "a revert outcome anchors even after an infra error")
}

// TestReactiveCallRetriesAfterAnchoredEvalError verifies that when an evaluation
// errors AFTER the subscription is anchored, the head loop keeps re-signaling it
// (needsReeval) so a result change whose evaluation transiently failed is still
// delivered — even on a later block with no dependency change, which otherwise
// would not re-signal an anchored, state-pure subscription.
func TestReactiveCallRetriesAfterAnchoredEvalError(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	dep := common.HexToAddress("0xa11")

	var mu sync.Mutex
	phase := 0 // 0 => ok "v1", 1 => transient error, 2 => ok "v2"
	eval := func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error) {
		mu.Lock()
		p := phase
		mu.Unlock()
		switch p {
		case 1:
			return nil, errors.New("eval timeout")
		case 2:
			return &reactiveEvalResult{returnData: []byte("v2"), deps: addrSet(dep), trackable: true}, nil
		default:
			return &reactiveEvalResult{returnData: []byte("v1"), deps: addrSet(dep), trackable: true}, nil
		}
	}
	m, be := newReactiveTestManager(t, eval)

	be.setHead(blockWithRoot(1, common.HexToHash("0x1")))
	sub := mustSubscribe(t, m, CallArgs{}, "retry")
	defer m.unsubscribe(sub)

	snap := recvReactive(t, sub)
	assert.Equal(t, "v1", string(snap.Result))

	// Block 2: a dep change triggers a re-eval, but the eval errors -> no emit,
	// needsReeval armed.
	mu.Lock()
	phase = 1
	mu.Unlock()
	root2 := commitAccountChange(t, dep, 2)
	be.setHead(blockWithRoot(2, root2))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(2, root2)})
	assertNoReactive(t, sub)

	// Block 3: a changeless block (no captured account change). Only needsReeval
	// can re-signal the anchored, state-pure subscription; the eval recovers and
	// the changed result is delivered.
	mu.Lock()
	phase = 2
	mu.Unlock()
	be.setHead(blockWithRoot(3, common.HexToHash("0x3")))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(3, common.HexToHash("0x3"))})

	upd := recvReactive(t, sub)
	assert.Equal(t, reactiveCallUpdate, upd.Type)
	assert.Equal(t, "v2", string(upd.Result))
}

// TestReactiveCallReevaluatesOnHeadJump verifies that when the head advances by
// more than one block (a batch insert emits a single ChainHeadEvent for the last
// block), every subscription is re-evaluated — so a result change in a skipped
// block is not missed even though only the final block's account changes are
// joined. Uses an empty dependency set so the ONLY thing that can trigger a
// re-evaluation is the head jump (no dep change, no block context, no pending
// retry — the initial snapshot does not grow an empty set).
func TestReactiveCallReevaluatesOnHeadJump(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)

	var mu sync.Mutex
	// Block 4 is never evaluated; the head jumps 2 -> 4 (skipping 3).
	results := map[uint64]string{1: "v1", 4: "v4"}
	m, be := newReactiveTestManager(t, staticEval(&mu, results, nil, true, nil))

	be.setHead(blockWithRoot(1, common.HexToHash("0x1")))
	sub := mustSubscribe(t, m, CallArgs{}, "jump")
	defer m.unsubscribe(sub)

	snap := recvReactive(t, sub)
	assert.Equal(t, "v1", string(snap.Result))
	require.False(t, sub.needsReeval.Load(), "an empty dependency set must not arm the arming-window retry")

	// A first (contiguous) head event establishes lastDispatched; an empty-dep,
	// state-pure, anchored subscription is not signaled, so nothing is emitted.
	be.setHead(blockWithRoot(2, common.HexToHash("0x2")))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(2, common.HexToHash("0x2"))})
	assertNoReactive(t, sub)

	// The head now jumps 2 -> 4 (block 3 skipped). The jump alone forces a re-eval,
	// surfacing the changed result.
	be.setHead(blockWithRoot(4, common.HexToHash("0x4")))
	be.feed.Send(blockchain.ChainHeadEvent{Block: blockWithRoot(4, common.HexToHash("0x4"))})

	upd := recvReactive(t, sub)
	assert.Equal(t, reactiveCallUpdate, upd.Type)
	assert.EqualValues(t, 1, upd.Seq)
	assert.Equal(t, "v4", string(upd.Result))
}

// TestReactiveCallArmedCapFallback verifies that a call whose accessed-account set
// drifts without bound stops growing the armed set at reactiveMaxArmedDeps and
// falls back to every-block re-evaluation (blockContextDep), staying correct via
// re-read + dedup. Driven by calling evaluate() directly for determinism.
func TestReactiveCallArmedCapFallback(t *testing.T) {
	defer state.SetBlockswordsAccountWatchlist(nil)
	prev := reactiveMaxArmedDeps
	reactiveMaxArmedDeps = 3
	defer func() { reactiveMaxArmedDeps = prev }()
	capHitsBefore := reactiveArmedCapHits.Count()

	// Each block reports a fresh dependency address, so the armed set grows by one
	// per block until it hits the cap.
	drift := func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error) {
		return &reactiveEvalResult{
			returnData: []byte("constant"),
			deps:       addrSet(common.BigToAddress(big.NewInt(int64(blockNum) + 1))),
			trackable:  true,
		}, nil
	}
	m, be := newReactiveTestManager(t, drift)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub := &reactiveCallSubscription{
		id:      1,
		rpcID:   "drift",
		mgr:     m,
		out:     make(chan *CallResultNotification, reactiveCallBacklog),
		trigger: make(chan struct{}, 1),
		ctx:     ctx,
		cancel:  cancel,
	}
	m.mu.Lock()
	m.subs[sub.id] = sub
	m.mu.Unlock()

	for n := uint64(1); n <= 8; n++ {
		be.setHead(blockWithRoot(n, common.BigToHash(big.NewInt(int64(n)))))
		sub.evaluate()
	}

	require.True(t, sub.armedCapped, "armed set must hit its cap and fall back")
	require.True(t, sub.blockContextDep.Load(), "capped subscription re-evaluates every block")
	require.LessOrEqual(t, len(sub.armed), reactiveMaxArmedDeps+1, "armed set is bounded near the cap")
	require.Greater(t, reactiveArmedCapHits.Count(), capHitsBefore, "armed-cap metric incremented")
}
