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
//      (dedup by outcome hash (return data + VM error) — stronger than slot-level
//      change detection).
//
// Result semantics: the subscription tracks the call's OUTCOME — the pair (return
// data, VM error) — and emits whenever it changes. A revert IS an outcome: a live
// kaia_call mirrors what a fresh kaia_call would return, and a poller would see the
// revert, so a success<->revert transition is emitted (Result empty, Error set),
// letting a client react when a vital call starts or stops failing. The snapshot is
// the first outcome (success OR revert); updates follow on change. Distinct from a
// revert is a backend EVALUATION error (the node could not run the call) — that is
// surfaced once as the optional leading "error" message, then retried, so a
// transient node hiccup is not mistaken for the call failing. Sequence:
// ["error"?] snapshot update*.
//
// Consistency: the initial eval uses a monotonic dependency-growth loop so the
// watch provably covers every block after the baseline (bounded by the call's
// footprint; no client retry, no unbounded spin). When an evaluation grows the
// dependency set, the engine forces one more re-evaluation on the next block
// (armingWindowReeval): a freshly-armed dependency might have changed in the brief
// window before it reached the global watch-list, and the next-block re-eval reads
// current state to catch it. A pathological non-converging dependency set falls
// back to self-healing — the next dependency change re-evaluates and corrects, and
// the dedup makes that a no-op when nothing actually changed.
//
// Architecture: the shared chain-head loop only *signals* subscriptions (a cheap
// ring lookup + set intersection); each subscription's own goroutine performs
// the EVM execution, so heavy calls never block the chain-head feed.
//
// Completeness: an update is emitted whenever the outcome changes (a changed
// return value, or a success<->revert transition). The outcome is a function of
// (a) the accounts it touches and (b) block context and (c) the call args (fixed
// for the subscription) — nothing else. Every account
// dependency is watched at account granularity, which covers ALL of an account's
// state: storage, balance, and code — including the dependencies no opcode
// exposes that BlockswordsCallDependencyTracer additionally captures: the key
// behind a validateSender call, the code behind an EIP-7702 delegation, and the
// existence of a CREATE/CREATE2 target. State changes signal via the account
// watch; block-context calls (Trackable=false: they read TIMESTAMP, NUMBER,
// BASEFEE, GASPRICE, ...) are re-evaluated on every block, since their result can
// change with no state change. Outcome dedup then emits only on a real change.
// A branch that newly reads an account is reached only via a watched state change
// or block context, both of which trigger a re-evaluation, so a shifting
// dependency set is discovered before it can be missed. A head that jumps over
// blocks (batch insert) re-evaluates every subscription. (This assumes IBFT
// immediate finality: there is no reorg machinery.)

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
	// reactiveCallSnapshot is the first OUTCOME message of a callResults
	// subscription: the call's current outcome (return value, or a revert) and the
	// anchor block. Subsequent messages cover strictly later blocks.
	reactiveCallSnapshot = "snapshot"
	// reactiveCallUpdate is a message emitted when the call's outcome changes (a new
	// return value, or a success<->revert transition).
	reactiveCallUpdate = "update"
	// reactiveCallError is a message emitted at most once, BEFORE the snapshot, when
	// the node cannot EVALUATE the call (a backend/state error or timeout) — an
	// infrastructure condition, NOT a revert. A revert is a normal call outcome and
	// is carried by a snapshot/update with the Error field set; this type is only
	// for "couldn't run the call", so a transient node hiccup is distinguishable
	// from the call genuinely failing. The reason is in Error; no value is carried.
	// The subscription stays open and retries; the snapshot follows once the call
	// can be evaluated.
	reactiveCallError = "error"

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
// The message sequence is: zero or one "error", then one "snapshot", then any
// number of "update"s — i.e. ["error"?] "snapshot" "update"*. Seq is a
// per-subscription counter incremented once per delivered "update" — contiguous,
// with no gaps: under rapid changes the engine coalesces to the latest outcome, and
// a send dropped because the consumer is briefly behind is re-attempted on the next
// block, so the consumer always converges to the current outcome. Trackable/
// BlockContext are set on the snapshot: Trackable is false when the call read
// block-context values (e.g. TIMESTAMP), meaning its result may change without any
// state change — the engine then re-evaluates it every block (so updates are still
// emitted), and BlockContext lists those opcodes.
//
// A subscription tracks the call's OUTCOME and emits whenever it changes. The
// "snapshot" (first outcome) and each "update" carry Result (the return data on
// success, empty on a revert) and Error (empty on success, e.g. "execution
// reverted" on a revert). A revert IS emitted — a live kaia_call mirrors what a
// fresh kaia_call would return, and a poller would see the revert — so a
// success<->revert transition is an update like any value change, letting a client
// react when a call starts or stops failing. (Two reverts with different reasons
// are not distinguished; see reactiveResultHash.) The "error" type is different: it
// is emitted only when the node cannot EVALUATE the call (backend/timeout) — an
// infra condition, not a call outcome — so a node hiccup is distinguishable from
// the call genuinely reverting.
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
// Protocol: the anchor message is {type:"snapshot", blockNumber:H, result, error,
// ...}, the call's first outcome (a return value, or a revert with error set);
// every later {type:"update", blockNumber:>H, result, error} reflects an outcome
// change — including a success<->revert transition, so a client can react when a
// vital call starts or stops failing. A leading {type:"error"} precedes the
// snapshot only when the node cannot evaluate the call (infra error, distinct from
// a revert), then retries. The full sequence is ["error"?] snapshot update*. The
// client thinks only in results — dependency discovery and watching are handled
// server-side and re-derived on every evaluation; the client need not poll. A
// snapshot trackable of
// false only means the result also depends on block context (e.g. TIMESTAMP), so
// the engine re-evaluates it every block instead of only on state changes.
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

	deps            atomic.Pointer[map[common.Address]struct{}] // current dependency accounts
	anchored        atomic.Bool                                 // snapshot emitted?
	blockContextDep atomic.Bool                                 // result reads block context => re-eval every block
	// needsReeval asks the head loop to re-evaluate this subscription on every
	// block until a fresh result is delivered, even with no watched change. Set
	// when the subscription may not reflect the current head: a send was dropped
	// (consumer behind), an evaluation errored (handleEvalError), or the dependency
	// set just grew (the arming-window re-eval, see evaluate). Cleared by the next
	// clean evaluation (delivered/unchanged success, or a revert).
	needsReeval atomic.Bool
	info        atomic.Pointer[CallResultSubscriptionInfo] // for the list method

	// out and trigger are never closed; the subscription is torn down by cancelling
	// ctx. This lets the head loop signal()/the eval goroutine emit() without
	// coordinating on close (a send to a cancelled subscription is a harmless,
	// never-read token), and lets the RPC reader goroutine select on ctx/Err only.
	out     chan *CallResultNotification // to the RPC notifier goroutine
	trigger chan struct{}                // cap 1, coalescing eval signal
	ctx     context.Context              // cancelled on unsubscribe
	cancel  context.CancelFunc

	// eval-owned (single goroutine):
	armed         map[common.Address]struct{} // monotonic watched set (never narrowed)
	published     map[common.Address]struct{} // last set pushed to the global watch-list
	lastHash      common.Hash
	seq           uint64
	errorReported bool // the one-time pre-snapshot infra "error" message was already emitted
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

// emit performs a non-blocking send to the delivery buffer and reports whether it
// was delivered. A full buffer means the consumer fell behind; rather than block
// the eval goroutine, the send is dropped. The caller then leaves the result
// undelivered (does not advance lastHash) and arms needsReeval so the head loop
// re-attempts it, re-pushing the latest result once the consumer catches up.
func (sub *reactiveCallSubscription) emit(n *CallResultNotification) bool {
	select {
	case sub.out <- n:
		return true
	default:
		return false
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

	// grew records whether this evaluation discovered a new dependency. If so, the
	// arming window must be closed: the new dependency reached the global watch-list
	// only just now, possibly after a block already committed without watching it,
	// so a change to it in that window would not have signaled the subscription. On
	// the convergent/fallback path below we therefore set needsReeval AFTER publish
	// (overriding publish's own clear), forcing one more re-evaluation on the next
	// block — by then every dependency is globally armed and the re-eval reads the
	// current head, catching any missed change; the next clean eval clears
	// needsReeval, so this adds at most one extra deduped evaluation per growth.
	grew := false
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
			if grew {
				sub.needsReeval.Store(true)
			}
			return
		}

		// New dependencies appeared: grow monotonically and re-confirm with the
		// larger set armed before the next head read.
		unionAddrInto(sub.armed, res.deps)
		grew = true
		if pass >= reactiveMaxStabilizationPasses {
			reactiveStabilizationFallbacks.Inc(1)
			logger.Warn("reactive call dependencies did not stabilize; relying on self-healing",
				"subID", sub.id, "deps", len(sub.armed))
			sub.armDeps()
			sub.publish(head, res)
			sub.needsReeval.Store(true) // grew; close the arming window (see grew above)
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

// handleEvalError reacts to a backend EVALUATION error — the node could not run the
// call (a backend/state error or an execution timeout). A revert is NOT such an
// error: it is a normal outcome that flows through publish with its Error field
// set. A cancelled context is silent (the subscription is shutting down). Otherwise
// it arms needsReeval so dispatch re-signals the subscription every block until an
// evaluation succeeds. This matters most for an already-anchored subscription: it
// would otherwise be re-signaled only by a fresh dependency change, so an outcome
// change whose evaluation transiently failed (e.g. a heavy call that timed out on
// the very block it changed) could be missed. An un-anchored subscription
// additionally emits the one-time infra "error" message, so the client is not left
// waiting while it retries.
func (sub *reactiveCallSubscription) handleEvalError(head *types.Block, err error) {
	if sub.ctx.Err() != nil {
		return
	}
	logger.Debug("reactive call evaluation failed; will retry", "subID", sub.id, "err", err)
	sub.needsReeval.Store(true)
	if sub.anchored.Load() || sub.errorReported {
		return
	}
	sub.errorReported = true
	n := &CallResultNotification{Type: reactiveCallError, Error: err.Error()}
	if head != nil {
		n.BlockNumber = hexutil.Uint64(head.NumberU64())
		n.BlockHash = head.Hash()
	}
	sub.emit(n)
}

type reactiveEvalResult struct {
	returnData   []byte // successful return data (meaningful only when vmErr == "")
	gasUsed      uint64
	vmErr        string // non-empty iff the call did not succeed (revert or VM error)
	deps         map[common.Address]struct{}
	trackable    bool
	blockContext []string
}

// reactiveEvaluator runs a call at a block and returns its result and the
// accounts it depends on. A non-nil error is a backend/execution error or
// cancellation (a reverted call is NOT an error: its vmErr is set and the
// dependencies up to the revert are still returned, so the engine keeps watching
// them and re-evaluates if the revert later clears). It is a field on the manager
// so tests can inject a deterministic evaluator in place of EVM execution. The
// returned result's deps map is owned by the caller.
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
		// The result is a function of every account the EVM touched. Most appear in
		// the access list (storage-accessed and called contracts); the tracer
		// additionally reports the accounts whose balance (incl. SELFBALANCE), code
		// (incl. EIP-7702 delegation targets), or key (validateSender) the result
		// depends on but the access list omits. All are watched at account
		// granularity. The engine also seeds the sender/recipient (read during
		// message setup, outside the EVM) — see reactiveCallSubscription.addArgSeeds.
		deps := accessListAddresses(tracer.AccessList())
		addAll := func(addrs []common.Address) {
			for _, addr := range addrs {
				deps[addr] = struct{}{}
			}
		}
		addAll(tracer.BalanceAddresses())
		addAll(tracer.CodeAddresses())
		addAll(tracer.KeyAddresses())
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

// publish reacts to an evaluation OUTCOME and emits when it changes. The outcome is
// the pair (return data, VM error): a successful call carries its return data with
// an empty error; a revert carries empty data with "execution reverted" (any other
// VM error likewise). Both are real outcomes of a live kaia_call — a client polling
// kaia_call would observe the revert — so a success<->revert transition is emitted
// like any value change, letting a client react to a call that starts (or stops)
// failing. The first outcome is the snapshot; each later change is an update,
// deduped by the outcome hash (gas excluded). A backend/eval error (the node could
// not run the call) is NOT an outcome and is handled by handleEvalError instead.
func (sub *reactiveCallSubscription) publish(head *types.Block, res *reactiveEvalResult) {
	// A block-context read (TIMESTAMP, NUMBER, GASPRICE, ...) — i.e. !trackable —
	// means the result can change every block with no state change, so mark the
	// subscription for the head loop to re-evaluate on every block (dedup then
	// emits only on a real change). Derived from the same trackable flag the
	// snapshot reports, so the two can never disagree. Monotonic: a conditionally-
	// read block-context value, once seen, keeps the sub on the every-block path.
	if !res.trackable {
		sub.blockContextDep.Store(true)
	}
	number := head.NumberU64()
	hash := reactiveResultHash(res.returnData, res.vmErr)

	// Advance lastHash/seq (and anchor) only on a delivered send. A send dropped
	// because the consumer's buffer is full leaves the outcome undelivered and arms
	// needsReeval, so the head loop re-attempts it (re-pushing the latest outcome)
	// on the next block rather than skipping it — see dispatch.
	if !sub.anchored.Load() {
		trackable := res.trackable
		if sub.emit(&CallResultNotification{
			Type:         reactiveCallSnapshot,
			BlockNumber:  hexutil.Uint64(number),
			BlockHash:    head.Hash(),
			Result:       res.returnData,
			GasUsed:      hexutil.Uint64(res.gasUsed),
			Trackable:    &trackable,
			BlockContext: res.blockContext,
			Error:        res.vmErr,
		}) {
			sub.lastHash = hash
			sub.anchored.Store(true)
			sub.needsReeval.Store(false) // delivered: clear any pending retry from an earlier failed eval
		}
		// On drop the subscription stays un-anchored, so dispatch keeps signaling
		// it every block until the snapshot is delivered.
	} else if hash != sub.lastHash {
		nextSeq := sub.seq + 1
		if sub.emit(&CallResultNotification{
			Type:        reactiveCallUpdate,
			Seq:         hexutil.Uint64(nextSeq),
			BlockNumber: hexutil.Uint64(number),
			BlockHash:   head.Hash(),
			Result:      res.returnData,
			GasUsed:     hexutil.Uint64(res.gasUsed),
			Error:       res.vmErr,
		}) {
			sub.lastHash = hash
			sub.seq = nextSeq
			sub.needsReeval.Store(false)
		} else {
			sub.needsReeval.Store(true)
		}
	} else {
		// Outcome unchanged and the client already has it: nothing pending.
		sub.needsReeval.Store(false)
	}

	sub.storeInfo(number, hash, res)
}

// storeInfo publishes the per-subscription status read by the list method.
func (sub *reactiveCallSubscription) storeInfo(number uint64, hash common.Hash, res *reactiveEvalResult) {
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

	// lastDispatched is the number of the most recently dispatched head block. It is
	// touched only by dispatch, which runs solely on the single head-loop goroutine,
	// so it needs no synchronization. Used to detect a multi-block head jump.
	lastDispatched uint64
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
	// Build AND publish the union under the lock. Publishing outside the lock would
	// race: a stale union (snapshotted before another goroutine armed a new dep)
	// could store last and drop that dep from the watch-list, silently missing its
	// changes. Serializing build+store means the last writer's union reflects every
	// sub's current deps. SetBlockswordsAccountWatchlist is a cheap atomic swap and
	// never calls back into the manager, so holding m.mu across it is safe.
	m.mu.Lock()
	defer m.mu.Unlock()
	union := make(map[common.Address]struct{})
	for _, sub := range m.subs {
		for addr := range sub.loadDeps() {
			union[addr] = struct{}{}
		}
	}
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
	number := block.NumberU64()
	// A normal live insert advances the head one block at a time, emitting one
	// ChainHeadEvent per block. A batch insert (e.g. an admin chain import) commits
	// several blocks but emits a single ChainHeadEvent for the last one, so the
	// per-block account-change ring entries for the skipped blocks are never looked
	// up here. Detect such a head jump and force EVERY subscription to re-evaluate
	// at the new head (dedup then emits only on a real change), so a dependency that
	// changed only in a skipped block is not missed. lastDispatched starts at zero
	// (no real block is 0), so the first dispatch is never treated as a jump.
	jumped := m.lastDispatched != 0 && number > m.lastDispatched+1
	m.lastDispatched = number

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
		// Signal a subscription when:
		//   - the head jumped over one or more blocks (batch insert): re-evaluate
		//     all subscriptions since the skipped blocks' changes were not joined; or
		//   - it is not yet anchored: every block gives a failed/pending initial
		//     evaluation another chance, even with no watched change; or
		//   - it reads block context: its result can change every block with no
		//     state change, so it must be re-evaluated every block (dedup then
		//     emits only on a real change); or
		//   - a retry is pending: the last evaluation errored or its send was
		//     dropped, so the client may be stale — re-attempt every block until a
		//     fresh result is delivered; or
		//   - one of its watched dependencies changed in this block.
		if jumped || !sub.anchored.Load() || sub.blockContextDep.Load() || sub.needsReeval.Load() ||
			(changedSet != nil && sub.dependsOnAny(changedSet)) {
			sub.signal()
		}
	}
}

// reactiveResultHash identifies a call OUTCOME by its return data and VM error,
// with a domain separator so the two cannot be confused. This makes a
// success<->revert transition a detected change (the VM error flips), while gas is
// excluded: it drifts with base fee and refunds without the outcome changing. Two
// reverts with different reasons hash the same (the return data is empty on any
// failure and the error string is the generic class), so a revert-reason change is
// not emitted — intended: the client gets the success<->failing signal, not a
// revert-payload diff. Use kaia_callWithAccessedStorage to inspect a revert reason.
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
