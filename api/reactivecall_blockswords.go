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

// Blockswords fork-only reactive ("live") kaia_call is kept in this file to
// minimize merge conflicts with upstream Kaia files.
//
// kaia_subscribe("callResults", callArgs) keeps a kaia_call's result live: the
// client receives the result once and again whenever it changes — thinking only
// in results, never in storage slots or filters. The engine:
//
//   1. runs the call and derives its dependencies from execution (the accessed
//      accounts — covering storage AND balances at account granularity);
//   2. watches those accounts via the account-level write hook;
//   3. on any dependency change, re-runs the call, re-derives dependencies
//      (they may shift), and pushes the result only if it actually changed
//      (dedup by result hash — stronger than slot-level change detection).
//
// Consistency: the initial eval uses a monotonic dependency-growth loop so the
// watch provably covers every block after the baseline (bounded by the call's
// footprint; no client retry, no unbounded spin). A pathological non-converging
// dependency set falls back to self-healing — the next dependency change
// re-evaluates and corrects, and the result-hash dedup makes that a no-op when
// nothing actually changed.
//
// Architecture: the shared chain-head loop only *signals* subscriptions (a cheap
// ring lookup + set intersection); each subscription's own goroutine performs
// the EVM execution, so heavy calls never block the chain-head feed.
//
// Limitations (a result can change without an update being emitted in these
// cases; the snapshot's Trackable flag covers the first):
//   - Block-context calls (Trackable=false): a result that reads TIMESTAMP,
//     NUMBER, BASEFEE, etc. may change every block independently of state. Such
//     calls update only on watched-state changes — poll them instead.
//   - EIP-7702: a re-delegation or cleared delegation IS tracked (it changes the
//     account's code), but a change to the *delegation target* contract's own
//     code is not tracked unless that target is otherwise touched.

import (
	"context"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/kaiachain/kaia/blockchain"
	"github.com/kaiachain/kaia/blockchain/state"
	"github.com/kaiachain/kaia/blockchain/types"
	"github.com/kaiachain/kaia/blockchain/vm"
	"github.com/kaiachain/kaia/common"
	"github.com/kaiachain/kaia/common/hexutil"
	"github.com/kaiachain/kaia/crypto"
	"github.com/kaiachain/kaia/networks/rpc"
	"github.com/kaiachain/kaia/params"
	"github.com/rcrowley/go-metrics"
)

const (
	// reactiveCallSnapshot is the first message of a callResults subscription:
	// the call's current result and the anchor block. Subsequent messages cover
	// strictly later blocks.
	reactiveCallSnapshot = "snapshot"
	// reactiveCallUpdate is a message emitted when the result changes.
	reactiveCallUpdate = "update"

	// reactiveMaxStabilizationPasses bounds the initial dependency-growth loop.
	// The dependency set is bounded by the call's footprint, so this is reached
	// only by a pathological call whose dependencies shift every block; we then
	// fall back to self-healing. Normal calls converge in 1-2 passes.
	reactiveMaxStabilizationPasses = 8

	// reactiveCallBacklog bounds the per-subscription delivery buffer.
	reactiveCallBacklog = 64
	// reactiveHeadBacklog bounds the shared chain-head channel.
	reactiveHeadBacklog = 128

	// reactiveMaxSubscriptions caps active reactive subscriptions node-wide.
	// Each one can drive a full EVM re-evaluation per dependency change, so this
	// guards a (trusted but careless) client from self-DoSing the node.
	reactiveMaxSubscriptions = 1024

	// reactiveDefaultGasCap bounds a single evaluation's gas when the node has
	// no global RPC gas cap configured.
	reactiveDefaultGasCap = 50_000_000
)

// reactiveEvalSem bounds the number of EVM evaluations running concurrently
// across all subscriptions, so a burst of dependency changes cannot spawn
// unbounded concurrent trie-walking calls and starve block processing. Sized to
// the CPU count: evaluations are CPU-bound, so more would only oversubscribe.
var reactiveEvalSem = make(chan struct{}, reactiveEvalConcurrency())

func reactiveEvalConcurrency() int {
	if n := runtime.GOMAXPROCS(0); n > 2 {
		return n
	}
	return 2
}

// reactiveStabilizationFallbacks counts how often the initial dependency-growth
// loop hit its pass cap and fell back to self-healing. A persistently nonzero
// rate points at calls whose dependency sets never stabilize.
var reactiveStabilizationFallbacks = metrics.NewRegisteredCounter("kaia/reactivecall/stabilization/fallback", nil)

// CallResultNotification is one message of a callResults subscription.
//
// Type is "snapshot" (initial) or "update". Seq is a per-subscription monotonic
// counter set on updates only; a gap means the consumer fell behind and should
// re-read (e.g. kaia_call) to resync — though the next change re-pushes the
// latest result anyway. Trackable/BlockContext are set on the snapshot:
// Trackable is false when the call read block-context values (e.g. TIMESTAMP),
// meaning its result may change without any tracked state change, and
// BlockContext lists those opcodes. Error carries a VM error (revert) or, on the
// snapshot, a terminal initial-evaluation error.
type CallResultNotification struct {
	Type         string         `json:"type"`
	Seq          hexutil.Uint64 `json:"seq,omitempty"`
	BlockNumber  hexutil.Uint64 `json:"blockNumber"`
	BlockHash    common.Hash    `json:"blockHash"`
	Result       hexutil.Bytes  `json:"result"`
	GasUsed      hexutil.Uint64 `json:"gasUsed"`
	Trackable    *bool          `json:"trackable,omitempty"`
	BlockContext []string       `json:"blockContext,omitempty"`
	Error        string         `json:"error,omitempty"`
}

// CallResultSubscriptionInfo describes one active reactive call, returned by
// kaia_callResultSubscriptions. It is node-global (the JSON-RPC layer does not
// expose a per-connection identity to non-subscription methods); a client
// matches entries to its own subscriptions via id.
type CallResultSubscriptionInfo struct {
	ID             rpc.ID         `json:"id"`
	LastBlock      hexutil.Uint64 `json:"lastBlock"`
	LastResultHash common.Hash    `json:"lastResultHash"`
	DepCount       int            `json:"depCount"`
	Trackable      bool           `json:"trackable"`
}

// CallResults keeps the result of a kaia_call live: it streams the result now
// and again whenever it changes. Exposed over WebSocket/IPC as
// kaia_subscribe("callResults", callArgs).
//
// Protocol: the first message is {type:"snapshot", blockNumber:H, result, ...};
// every later {type:"update", blockNumber:>H, result} reflects a change. The
// client thinks only in results — dependency discovery and watching are handled
// server-side and re-derived on every evaluation. If the snapshot's trackable is
// false the result also depends on block context (e.g. TIMESTAMP) and may change
// without a state change; the client should poll such calls instead of relying
// on this stream alone.
func (s *KaiaBlockChainAPI) CallResults(ctx context.Context, args CallArgs) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	rpcSub := notifier.CreateSubscription()

	mgr := reactiveCallManagerFor(s.b)
	sub, err := mgr.subscribe(args, rpcSub.ID)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			select {
			case notification := <-sub.out:
				notifier.Notify(rpcSub.ID, notification)
			case <-rpcSub.Err():
				mgr.unsubscribe(sub)
				return
			case <-notifier.Closed():
				mgr.unsubscribe(sub)
				return
			}
		}
	}()

	return rpcSub, nil
}

// CallResultSubscriptions lists the active reactive call subscriptions on this
// node (see CallResultSubscriptionInfo for the per-connection caveat).
func (s *KaiaBlockChainAPI) CallResultSubscriptions(ctx context.Context) []CallResultSubscriptionInfo {
	mgr := reactiveCallManagerFor(s.b)
	return mgr.list()
}

// reactiveCallSubscription is one live call. The fields under "eval-owned" are
// touched only by the subscription's single eval goroutine and need no
// synchronization; the atomic fields are published by that goroutine and read by
// the shared head loop and the list method.
type reactiveCallSubscription struct {
	id    uint64
	rpcID rpc.ID
	args  CallArgs
	mgr   *reactiveCallManager

	deps     atomic.Pointer[map[common.Address]struct{}] // current dependency accounts
	anchored atomic.Bool                                 // snapshot emitted?
	info     atomic.Pointer[CallResultSubscriptionInfo]  // for the list method

	out     chan *CallResultNotification // to the RPC notifier goroutine
	trigger chan struct{}                // cap 1, coalescing eval signal
	ctx     context.Context              // cancelled on unsubscribe
	cancel  context.CancelFunc

	// eval-owned (single goroutine):
	armed         map[common.Address]struct{} // monotonic watched set (never narrowed)
	published     map[common.Address]struct{} // last set pushed to the global watch-list
	lastHash      common.Hash
	seq           uint64
	errorReported bool // a terminal initial-eval error was already surfaced
}

func (sub *reactiveCallSubscription) loadDeps() map[common.Address]struct{} {
	if p := sub.deps.Load(); p != nil {
		return *p
	}
	return nil
}

// dependsOnAny reports whether any of the changed addresses is a dependency.
func (sub *reactiveCallSubscription) dependsOnAny(changed map[common.Address]struct{}) bool {
	deps := sub.loadDeps()
	// Iterate the smaller set.
	if len(changed) < len(deps) {
		for addr := range changed {
			if _, ok := deps[addr]; ok {
				return true
			}
		}
		return false
	}
	for addr := range deps {
		if _, ok := changed[addr]; ok {
			return true
		}
	}
	return false
}

// signal requests an evaluation; coalesced if one is already pending.
func (sub *reactiveCallSubscription) signal() {
	select {
	case sub.trigger <- struct{}{}:
	default:
	}
}

// emit performs a non-blocking send to the delivery buffer. A full buffer means
// the consumer fell behind; the message is dropped (the advanced seq exposes the
// gap on updates) rather than blocking the eval goroutine.
func (sub *reactiveCallSubscription) emit(n *CallResultNotification) {
	select {
	case sub.out <- n:
	default:
	}
}

// run is the subscription's eval goroutine: an initial evaluation (snapshot)
// followed by re-evaluations on every trigger until cancelled.
func (sub *reactiveCallSubscription) run() {
	sub.evaluate()
	for {
		select {
		case <-sub.trigger:
			sub.evaluate()
		case <-sub.ctx.Done():
			return
		}
	}
}

// evaluate runs the call at the current head, growing the armed dependency set
// until the watch provably covers the result's block, then publishes if the
// result changed. See the file header for the consistency argument.
//
// The armed set grows monotonically and is never narrowed: dropping a
// conditionally-read dependency (one read only on some code path) would miss
// updates that re-introduce it. Over-watching only costs extra, hash-deduped
// re-evaluations.
func (sub *reactiveCallSubscription) evaluate() {
	// Bound concurrent EVM evaluations across all subscriptions.
	select {
	case reactiveEvalSem <- struct{}{}:
		defer func() { <-reactiveEvalSem }()
	case <-sub.ctx.Done():
		return
	}

	if sub.armed == nil {
		sub.armed = cloneAddrSet(sub.loadDeps())
	}

	for pass := 0; ; pass++ {
		// Arm the watch on the current set *before* reading the head, so the
		// watch covers every block after the block we evaluate at. Idempotent:
		// re-publishes only when the set actually changed.
		sub.armDeps()

		head := sub.mgr.backend.CurrentBlock()
		if head == nil {
			return
		}
		res, err := sub.mgr.eval(sub.ctx, sub.args, head.NumberU64())
		if err != nil {
			sub.handleEvalError(head, err)
			return
		}
		sub.addArgSeeds(res.deps)

		if isAddrSubset(res.deps, sub.armed) {
			sub.publish(head, res)
			return
		}

		// New dependencies appeared: grow monotonically and re-confirm with the
		// larger set armed before the next head read.
		unionAddrInto(sub.armed, res.deps)
		if pass >= reactiveMaxStabilizationPasses {
			reactiveStabilizationFallbacks.Inc(1)
			logger.Warn("reactive call dependencies did not stabilize; relying on self-healing",
				"subID", sub.id, "deps", len(sub.armed))
			sub.armDeps()
			sub.publish(head, res)
			return
		}
	}
}

// addArgSeeds adds the call's sender and recipient to deps. Their balance and
// nonce are read during message setup (in DoCall/StateTransition) outside the
// EVM, so the tracer never sees them, yet the result can depend on them (e.g. a
// value-bearing call gated by the sender's balance).
func (sub *reactiveCallSubscription) addArgSeeds(deps map[common.Address]struct{}) {
	if (sub.args.From != common.Address{}) {
		deps[sub.args.From] = struct{}{}
	}
	if sub.args.To != nil {
		deps[*sub.args.To] = struct{}{}
	}
}

// armDeps publishes the current armed set to the head loop and the global
// account watch-list, but only when it actually changed since the last publish —
// so a steady-state re-evaluation does no watch-list work.
func (sub *reactiveCallSubscription) armDeps() {
	if equalAddrSet(sub.armed, sub.published) {
		return
	}
	sub.published = cloneAddrSet(sub.armed)
	cp := cloneAddrSet(sub.armed)
	sub.deps.Store(&cp)
	sub.mgr.refreshWatchlist()
}

// handleEvalError reacts to a terminal evaluation error. A cancelled context is
// silent (the subscription is shutting down). Otherwise, if no snapshot has been
// sent yet, surface the error once so the client is not left hanging; the
// subscription remains un-anchored so dispatch retries it on the next block.
func (sub *reactiveCallSubscription) handleEvalError(head *types.Block, err error) {
	if sub.ctx.Err() != nil {
		return
	}
	logger.Debug("reactive call evaluation failed; will retry", "subID", sub.id, "err", err)
	if sub.anchored.Load() || sub.errorReported {
		return
	}
	sub.errorReported = true
	n := &CallResultNotification{Type: reactiveCallSnapshot, Error: err.Error()}
	if head != nil {
		n.BlockNumber = hexutil.Uint64(head.NumberU64())
		n.BlockHash = head.Hash()
	}
	sub.emit(n)
}

type reactiveEvalResult struct {
	returnData   []byte
	gasUsed      uint64
	vmErr        string
	deps         map[common.Address]struct{}
	trackable    bool
	blockContext []string
}

// reactiveEvaluator runs a call at a block and returns its result and the
// accounts it depends on. A non-nil error is a backend/execution error or
// cancellation (a reverted call is NOT an error: its vmErr is captured and
// dependencies up to the revert are still returned). It is a field on the
// manager so tests can inject a deterministic evaluator in place of EVM
// execution. The returned result's deps map is owned by the caller.
type reactiveEvaluator func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error)

// newReactiveEvaluator returns the production evaluator: a single kaia_call with
// the dependency tracer.
func newReactiveEvaluator(b Backend) reactiveEvaluator {
	return func(ctx context.Context, args CallArgs, blockNum uint64) (*reactiveEvalResult, error) {
		// Bound a single evaluation's cost even when no global cap is set, so a
		// reactive subscription on a snapshot-less node cannot run unbounded gas.
		gasCap := big.NewInt(reactiveDefaultGasCap)
		if rpcGasCap := b.RPCGasCap(); rpcGasCap != nil {
			gasCap = rpcGasCap
		}
		tracer := vm.NewBlockswordsCallDependencyTracer(nil)
		vmCfg := vm.Config{
			Debug:                true,
			Tracer:               tracer,
			ComputationCostLimit: params.OpcodeComputationCostLimitInfinite,
			UseConsoleLog:        b.IsConsoleLogEnabled(),
		}
		blockNrOrHash := rpc.NewBlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum))
		result, _, err := DoCall(ctx, b, args, blockNrOrHash, vmCfg, b.RPCEVMTimeout(), gasCap)
		if err != nil {
			return nil, err
		}
		// The result is a function of every account the EVM touched: storage-
		// accessed contracts (the access list) and balance-read accounts
		// (including SELFBALANCE, which the access list omits). The engine
		// additionally seeds the sender/recipient (read during message setup,
		// outside the EVM) — see reactiveCallSubscription.addArgSeeds.
		deps := accessListAddresses(tracer.AccessList())
		for _, addr := range tracer.BalanceAddresses() {
			deps[addr] = struct{}{}
		}
		res := &reactiveEvalResult{
			returnData:   result.Return(),
			gasUsed:      result.UsedGas,
			deps:         deps,
			trackable:    tracer.Trackable(),
			blockContext: tracer.BlockContextOpcodes(),
		}
		if vmErr := result.Unwrap(); vmErr != nil {
			res.vmErr = vmErr.Error()
		}
		return res, nil
	}
}

// publish emits the snapshot (first time) or an update when the result changed.
// The result hash covers the return data and the VM error, so success<->revert
// transitions are detected; gas is not part of the result identity.
func (sub *reactiveCallSubscription) publish(head *types.Block, res *reactiveEvalResult) {
	hash := reactiveResultHash(res.returnData, res.vmErr)
	number := head.NumberU64()

	if !sub.anchored.Load() {
		sub.lastHash = hash
		trackable := res.trackable
		sub.emit(&CallResultNotification{
			Type:         reactiveCallSnapshot,
			BlockNumber:  hexutil.Uint64(number),
			BlockHash:    head.Hash(),
			Result:       res.returnData,
			GasUsed:      hexutil.Uint64(res.gasUsed),
			Trackable:    &trackable,
			BlockContext: res.blockContext,
			Error:        res.vmErr,
		})
		sub.anchored.Store(true)
	} else if hash != sub.lastHash {
		sub.lastHash = hash
		sub.seq++
		sub.emit(&CallResultNotification{
			Type:        reactiveCallUpdate,
			Seq:         hexutil.Uint64(sub.seq),
			BlockNumber: hexutil.Uint64(number),
			BlockHash:   head.Hash(),
			Result:      res.returnData,
			GasUsed:     hexutil.Uint64(res.gasUsed),
			Error:       res.vmErr,
		})
	}

	sub.info.Store(&CallResultSubscriptionInfo{
		ID:             sub.rpcID,
		LastBlock:      hexutil.Uint64(number),
		LastResultHash: hash,
		DepCount:       len(res.deps),
		Trackable:      res.trackable,
	})
}

// reactiveCallManager owns a single ChainHeadEvent subscription and signals
// reactive subscriptions whose dependencies changed. It is a process singleton.
type reactiveCallManager struct {
	backend Backend
	eval    reactiveEvaluator

	mu      sync.Mutex
	subs    map[uint64]*reactiveCallSubscription
	nextID  uint64
	running bool
	quit    chan struct{}
}

var (
	reactiveCallManagerOnce sync.Once
	reactiveCallManagerInst *reactiveCallManager
)

func reactiveCallManagerFor(b Backend) *reactiveCallManager {
	reactiveCallManagerOnce.Do(func() {
		reactiveCallManagerInst = &reactiveCallManager{
			backend: b,
			eval:    newReactiveEvaluator(b),
			subs:    make(map[uint64]*reactiveCallSubscription),
		}
	})
	return reactiveCallManagerInst
}

func (m *reactiveCallManager) subscribe(args CallArgs, rpcID rpc.ID) (*reactiveCallSubscription, error) {
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	if len(m.subs) >= reactiveMaxSubscriptions {
		m.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("callResults: too many active subscriptions (max %d)", reactiveMaxSubscriptions)
	}
	m.nextID++
	sub := &reactiveCallSubscription{
		id:      m.nextID,
		rpcID:   rpcID,
		args:    args,
		mgr:     m,
		out:     make(chan *CallResultNotification, reactiveCallBacklog),
		trigger: make(chan struct{}, 1),
		ctx:     ctx,
		cancel:  cancel,
	}
	m.subs[sub.id] = sub
	if !m.running {
		m.startLocked() // start once; the head loop runs for the manager's lifetime
	}
	m.mu.Unlock()

	go sub.run()
	return sub, nil
}

func (m *reactiveCallManager) unsubscribe(sub *reactiveCallSubscription) {
	m.mu.Lock()
	if _, ok := m.subs[sub.id]; !ok {
		m.mu.Unlock()
		return
	}
	delete(m.subs, sub.id)
	m.mu.Unlock()

	sub.cancel()         // stop the eval goroutine and abort any in-flight call
	m.refreshWatchlist() // recompute the union without this sub (clears it when last)
}

// refreshWatchlist recomputes the union of every subscription's dependency
// accounts and pushes it to the state-layer account capture gate.
func (m *reactiveCallManager) refreshWatchlist() {
	m.mu.Lock()
	union := make(map[common.Address]struct{})
	for _, sub := range m.subs {
		for addr := range sub.loadDeps() {
			union[addr] = struct{}{}
		}
	}
	m.mu.Unlock()

	addrs := make([]common.Address, 0, len(union))
	for addr := range union {
		addrs = append(addrs, addr)
	}
	state.SetBlockswordsAccountWatchlist(addrs)
}

func (m *reactiveCallManager) list() []CallResultSubscriptionInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]CallResultSubscriptionInfo, 0, len(m.subs))
	for _, sub := range m.subs {
		if info := sub.info.Load(); info != nil {
			out = append(out, *info)
		}
	}
	return out
}

// startLocked starts the shared chain-head loop once. Must hold m.mu. The loop
// runs for the manager's lifetime (the manager is a process singleton); we never
// stop-and-restart it, which avoids a restart race where two head loops could
// briefly contend for the same per-block delta. When no subscriptions exist the
// account watch-list is empty, so the loop idles cheaply (ring lookups return
// nil). quit exists only for test cleanup via close.
func (m *reactiveCallManager) startLocked() {
	headCh := make(chan blockchain.ChainHeadEvent, reactiveHeadBacklog)
	headSub := m.backend.SubscribeChainHeadEvent(headCh)
	quit := make(chan struct{})
	m.quit = quit
	m.running = true

	go func() {
		defer headSub.Unsubscribe()
		for {
			select {
			case ev := <-headCh:
				m.dispatch(ev.Block)
			case <-headSub.Err():
				return
			case <-quit:
				return
			}
		}
	}()
}

// close stops the chain-head loop. It is intended for test cleanup; production
// uses the process-lifetime singleton and never calls it.
func (m *reactiveCallManager) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		m.running = false
		close(m.quit)
	}
}

// dispatch signals subscriptions affected by the new canonical block. It only
// does cheap work (a ring lookup, set intersection, and non-blocking signals);
// the actual re-evaluation happens in each subscription's own goroutine, so a
// slow or heavy call never stalls block processing.
func (m *reactiveCallManager) dispatch(block *types.Block) {
	changed := state.LookupBlockswordsAccountChanges(block.Root())
	var changedSet map[common.Address]struct{}
	if len(changed) > 0 {
		changedSet = make(map[common.Address]struct{}, len(changed))
		for _, addr := range changed {
			changedSet[addr] = struct{}{}
		}
	}

	m.mu.Lock()
	subs := make([]*reactiveCallSubscription, 0, len(m.subs))
	for _, sub := range m.subs {
		subs = append(subs, sub)
	}
	m.mu.Unlock()

	for _, sub := range subs {
		// Signal a subscription when one of its dependencies changed, and signal
		// not-yet-anchored subscriptions on every block so a failed (or pending)
		// initial evaluation always gets a chance to retry — even on a block with
		// no watched changes.
		if !sub.anchored.Load() || (changedSet != nil && sub.dependsOnAny(changedSet)) {
			sub.signal()
		}
	}
}

// reactiveResultHash identifies a call result by its return data and VM error,
// with a domain separator so data and error cannot be confused.
func reactiveResultHash(returnData []byte, vmErr string) common.Hash {
	return crypto.Keccak256Hash(returnData, []byte{0}, []byte(vmErr))
}

// accessListAddresses returns every address in the access list as a set. These
// are the call's dependency accounts (watched at account granularity, which
// covers their storage, balance and code).
func accessListAddresses(al types.AccessList) map[common.Address]struct{} {
	out := make(map[common.Address]struct{}, len(al))
	for _, tuple := range al {
		out[tuple.Address] = struct{}{}
	}
	return out
}

func cloneAddrSet(in map[common.Address]struct{}) map[common.Address]struct{} {
	out := make(map[common.Address]struct{}, len(in))
	for addr := range in {
		out[addr] = struct{}{}
	}
	return out
}

func isAddrSubset(sub, super map[common.Address]struct{}) bool {
	if len(sub) > len(super) {
		return false
	}
	for addr := range sub {
		if _, ok := super[addr]; !ok {
			return false
		}
	}
	return true
}

func equalAddrSet(a, b map[common.Address]struct{}) bool {
	return len(a) == len(b) && isAddrSubset(a, b)
}

func unionAddrInto(dst, src map[common.Address]struct{}) {
	for addr := range src {
		dst[addr] = struct{}{}
	}
}
