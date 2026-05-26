# Blockswords RPC reference

Fork-only JSON-RPC methods added by the `blockswords` fork for indexing/state
monitoring. All methods live on the `KaiaBlockChainAPI` service and are therefore
exposed under the **`kaia`**, **`klay`**, and **`eth`** namespaces (examples below
use `kaia`).

| Method | Kind | Transport |
|---|---|---|
| [`kaia_callWithAccessedStorage`](#kaia_callwithaccessedstorage) | request/response | HTTP, WS, IPC |
| [`kaia_subscribe("storageChanges", …)`](#kaia_subscribestoragechanges-filters) | subscription | WS, IPC |
| [`kaia_subscribe("callResults", …)`](#kaia_subscribecallresults-callargs) | subscription | WS, IPC |
| [`kaia_callResultSubscriptions`](#kaia_callresultsubscriptions) | request/response | HTTP, WS, IPC |

**General notes**

- Subscriptions require **WebSocket or IPC**; over HTTP they return
  `notifications not supported`. Enable the `kaia` (and/or `eth`) module on the WS
  endpoint.
- Capture is **gated by a lock-free atomic watch-list and is a no-op when no
  subscription is active** — the methods add no measurable cost to block import
  when unused.
- Behaviour is **independent of `--snapshot`** (capture reads no state; reactive
  evaluation uses the normal call path).
- These methods are **fork-only and unstable**; field names may change.

---

## `kaia_callWithAccessedStorage`

Executes a call exactly like `kaia_call`, and additionally reports the
**dependencies** the call touched: accessed storage slots, balance reads, and any
block-context reads. Use it to discover what a call depends on (e.g. to drive a
`callResults` or `storageChanges` subscription, or your own cache invalidation).

### Parameters

1. `callArgs` — `Object`. Same as `kaia_call` (see [CallArgs](#callargs)).
2. `blockNumberOrHash` — `String` | `Object`. `"latest"`, a hex block number,
   or `{ "blockHash": "0x…" }`.

### Returns

`Object`:

| field | type | description |
|---|---|---|
| `return` | `Data` | Call return data (empty if the call reverted or errored). |
| `gasUsed` | `Quantity` | Gas consumed. |
| `accessList` | `Array<AccessTuple>` | EIP-2930 access list: addresses and storage slots the call read or wrote. See [AccessTuple](#accesstuple). |
| `balanceAddresses` | `Array<Address>` | Addresses whose **balance** the call read (`BALANCE`/`SELFBALANCE`). A non-empty list makes `storageTrackable` false (balances change outside the storage trie). Omitted if empty. |
| `codeAddresses` | `Array<Address>` | Addresses whose **code/existence** the result depends on: those read via `EXTCODESIZE`/`EXTCODEHASH`/`EXTCODECOPY`, the target of any **EIP-7702 delegation** the call invokes, and the address any **`CREATE`/`CREATE2`** would deploy to (its prior occupancy gates the create). A non-empty list makes `storageTrackable` false (code/existence changes are invisible to a storage watch). Omitted if empty. |
| `keyAddresses` | `Array<Address>` | Accounts whose **key** the result depends on: the `from` of any `validateSender` precompile call. A key changes on an `AccountUpdate` (outside the storage trie), so a non-empty list makes `storageTrackable` false. Omitted if empty. |
| `blockContext` | `Array<String>` | Block-context opcodes the call used (e.g. `"TIMESTAMP"`, `"NUMBER"`, `"BASEFEE"`, and `"GASPRICE"` — which on Kaia defaults to `baseFee*2`). Omitted if empty. |
| `storageTrackable` | `Boolean` | `true` iff the result can be kept live by watching `accessList` via `storageChanges` — i.e. a pure function of **storage**: `blockContext`, `balanceAddresses`, `codeAddresses`, **and** `keyAddresses` are all empty. When `false`, watch the call with `callResults` (account granularity) or poll. **Distinct from** the `callResults` snapshot's `trackable` (a weaker predicate — see that method); do not equate the two when seeding a `callResults` watch from this method. |
| `error` | `String` | VM error (e.g. `"execution reverted"`), if any. Omitted if none. The access list is still returned for a reverted call (slots touched up to the revert). |

### Example

```json
// request
{ "jsonrpc":"2.0", "id":1, "method":"kaia_callWithAccessedStorage",
  "params":[ { "to":"0xPair", "data":"0x0902f1 b…" }, "latest" ] }

// response
{ "jsonrpc":"2.0", "id":1, "result": {
  "return": "0x…",
  "gasUsed": "0x6f3c",
  "accessList": [
    { "address":"0xPair", "storageKeys":["0x8","0x9","0xc"] }
  ],
  "balanceAddresses": [],
  "codeAddresses": [],
  "keyAddresses": [],
  "storageTrackable": true
} }
```

### Notes

- A single execution; unlike `eth_createAccessList` it does **not** iterate to a
  gas fixpoint — it only records what the call depended on.
- `balanceAddresses` is a strict classification of balance reads;
  `SELFBALANCE` targets appear here but not in `accessList`.
- `storageTrackable` is **storage**-trackability: it answers "is watching
  `accessList` via `storageChanges` enough?". It is intentionally stricter than the
  account-granularity `trackable` reported by `callResults`: a result that depends
  on a balance, code/existence, or account key is `storageTrackable:false` here
  (a storage-only watch can't see those) yet is fully tracked by `callResults`
  (where its `trackable` would be `true`). The two fields are named differently on
  purpose — do not equate them.
  `balanceAddresses`/`codeAddresses`/`keyAddresses`/`blockContext` tell you
  exactly which reason applies.
- `storageTrackable` treats the **code of an ordinary contract the call invokes** (`CALL`/
  `STATICCALL`/`DELEGATECALL`) as immutable — true for deployed contracts, whose
  behavior then depends only on their (captured) storage. EIP-7702 delegation
  targets and `validateSender` account keys **are** captured (in `codeAddresses`/
  `keyAddresses`); only a regular callee being deployed-into or
  self-destructed+redeployed is not reflected here. If a call's result can hinge
  on that, use `callResults` (account granularity tracks callee code).

---

## `kaia_subscribe("storageChanges", filters)`

Streams the **net per-block storage changes** for the watched contracts/slots.
Changes are captured at the state-write commit point (no state reads), reported
once per block, with intra-block round-trips collapsed.

### Parameters

1. `"storageChanges"` — subscription name.
2. `filters` — `Array<Object>`, at least one entry:

   | field | type | description |
   |---|---|---|
   | `address` | `Address` | Contract to watch. |
   | `slots` | `Array<Hash>` | Specific storage slots. **Omit/empty ⇒ all slots** of `address` (contract granularity). |

### Notification frames

The first message is a `ready` anchor; subsequent messages are `changes`.

**`ready`** (sent once, on the first observed block):

| field | type | description |
|---|---|---|
| `type` | `String` | `"ready"`. |
| `blockNumber` | `Quantity` | **Anchor block `H`.** Take your cold-start snapshot pinned at `H`. |
| `blockHash` | `Hash` | Hash of block `H`. |

**`changes`** (one per block that changed a watched slot, all with `blockNumber > H`):

| field | type | description |
|---|---|---|
| `type` | `String` | `"changes"`. |
| `seq` | `Quantity` | Per-subscription monotonic counter starting at 1. A gap means messages were dropped (slow consumer) — resync. |
| `blockNumber` | `Quantity` | Block number. |
| `blockHash` | `Hash` | Block hash. |
| `changes` | `Array<Object>` | `{ address, key, previous, value }` per changed slot. `value = 0x0…0` means the slot was cleared. |

### Consistent cold-start protocol

1. **Subscribe first.**
2. On the `ready` frame, read your cold-start snapshot **pinned at block `H`**
   (pass `H` to `kaia_getStoragesAt`/`kaia_call`, never `"latest"`).
3. Apply every subsequent `changes` frame (all cover blocks `> H`). Idempotent
   upsert is safe.

Block `H`'s own changes are delivered via your snapshot (not the stream), which
removes the snapshot/subscribe gap.

### Example

```json
// request
{ "jsonrpc":"2.0", "id":1, "method":"kaia_subscribe",
  "params":[ "storageChanges", [ { "address":"0xPair", "slots":["0x8"] } ] ] }
// response: { "jsonrpc":"2.0","id":1,"result":"0x9ce…" }   (subscription id)

// ready frame
{ "jsonrpc":"2.0","method":"kaia_subscription","params":{ "subscription":"0x9ce…",
  "result": { "type":"ready", "blockNumber":"0x12a0c4", "blockHash":"0x…" } } }

// changes frame
{ "jsonrpc":"2.0","method":"kaia_subscription","params":{ "subscription":"0x9ce…",
  "result": { "type":"changes", "seq":"0x1", "blockNumber":"0x12a0c5", "blockHash":"0x…",
    "changes":[ { "address":"0xPair", "key":"0x8",
                  "previous":"0x…aaa", "value":"0x…bbb" } ] } } }
```

### Notes

- **Known limitation:** a `SELFDESTRUCT`-driven storage wipe is **not** reported.
  Do not use `storageChanges` for contracts that may self-destruct.
- On a `seq` gap or disconnect, resync via `debug_diffContractStorageHash` from
  your last block, then resubscribe.

---

## `kaia_subscribe("callResults", callArgs)`

A **reactive ("live") `kaia_call`**: streams the call's current **outcome**, then a
new one **whenever it changes**. The server derives the call's dependency accounts
from execution, watches them, re-evaluates on any change, and pushes only when the
outcome actually changes (deduped by outcome hash). The client deals only in
results — no filters, no storage slots.

The outcome is the pair **(return data, error)**: a success carries its return data
with an empty error; a **revert carries an empty result with `error` set**. A
revert IS emitted — a live `kaia_call` mirrors what a fresh `kaia_call` would
return, and a poller would see the revert — so a **success↔revert transition is an
`update`** like any value change. That lets an app react when a vital call starts
or stops failing (e.g. temporarily blacklist it). Two reverts with *different*
reasons are not distinguished (use `kaia_callWithAccessedStorage` to inspect a
revert reason). Separately, a backend **evaluation** error (the node could not run
the call) is surfaced as the distinct `error` message — not to be confused with a
revert.

### Parameters

1. `"callResults"` — subscription name.
2. `callArgs` — `Object`. Same as `kaia_call` (see [CallArgs](#callargs)). Always
   evaluated at the **latest** canonical head.

### Notification frames

The message sequence is **`["error"?] snapshot update*`**: an optional one-time
`error` (only if the node cannot evaluate the call), then exactly one `snapshot`
(the first outcome), then an `update` per outcome change.

**`error`** (at most once, **before** the `snapshot`):

| field | type | description |
|---|---|---|
| `type` | `String` | `"error"`. |
| `blockNumber` | `Quantity` | Block evaluated at. |
| `blockHash` | `Hash` | That block's hash. |
| `error` | `String` | An **infrastructure** error — the node could not run the call (backend unavailable, state pruned, timeout). **Not** a revert: a revert is a normal outcome carried by `snapshot`/`update` with `error` set. The subscription stays open and retries; the `snapshot` follows once the call can be evaluated. |

**`snapshot`** (sent once, on the first evaluable outcome — success **or** revert):

| field | type | description |
|---|---|---|
| `type` | `String` | `"snapshot"`. |
| `blockNumber` | `Quantity` | Block the result was evaluated at. |
| `blockHash` | `Hash` | That block's hash. |
| `result` | `Data` | Call return data (empty on a revert). |
| `error` | `String` | Empty on success; the VM error (e.g. `"execution reverted"`) if the call reverts. Omitted when empty. |
| `gasUsed` | `Quantity` | Gas used. |
| `trackable` | `Boolean` | `false` ⇒ the call reads block-context (see `blockContext`); its result can change without a state change, so the engine **re-evaluates it every block** instead of only on state changes. The stream stays complete either way — no polling needed. |
| `blockContext` | `Array<String>` | Block-context opcodes detected (present only when `trackable=false`). |

**`update`** (one per block where the outcome changed — incl. a success↔revert flip):

| field | type | description |
|---|---|---|
| `type` | `String` | `"update"`. |
| `seq` | `Quantity` | Per-subscription counter of **delivered** updates, starting at 1 — contiguous, no gaps. Under rapid changes the engine coalesces to the latest outcome, and a send dropped because your buffer is briefly full is re-attempted on the next block, so you always converge to the current outcome. |
| `blockNumber` | `Quantity` | Block the new result was evaluated at. |
| `blockHash` | `Hash` | That block's hash. |
| `result` | `Data` | New return data (empty on a revert). |
| `error` | `String` | The VM error if the call now reverts; empty on success. Omitted when empty. |
| `gasUsed` | `Quantity` | Gas used. |

The outcome identity is `keccak256(result ‖ error)`; `gasUsed` is **not** part of
it (gas can wobble without the outcome changing). A success↔revert flip changes the
`error` and so is emitted; two reverts with different reasons hash the same (empty
result, same generic error) and are **not** re-emitted.

### Example

```json
// request
{ "jsonrpc":"2.0","id":1,"method":"kaia_subscribe",
  "params":[ "callResults", { "to":"0xPair", "data":"0x0902f1 b…" } ] }
// response: { "jsonrpc":"2.0","id":1,"result":"0x4f1…" }

// snapshot frame
{ "jsonrpc":"2.0","method":"kaia_subscription","params":{ "subscription":"0x4f1…",
  "result": { "type":"snapshot", "blockNumber":"0x12a0c4", "blockHash":"0x…",
    "result":"0x…reserves…", "gasUsed":"0x6f3c", "trackable":true } } }

// update frame (reserves changed)
{ "jsonrpc":"2.0","method":"kaia_subscription","params":{ "subscription":"0x4f1…",
  "result": { "type":"update", "seq":"0x1", "blockNumber":"0x12a0c9", "blockHash":"0x…",
    "result":"0x…new reserves…", "gasUsed":"0x6f3c" } } }

// update frame (the call now reverts — react, e.g. blacklist)
{ "jsonrpc":"2.0","method":"kaia_subscription","params":{ "subscription":"0x4f1…",
  "result": { "type":"update", "seq":"0x2", "blockNumber":"0x12a0d0", "blockHash":"0x…",
    "result":"0x", "error":"execution reverted", "gasUsed":"0x6f3c" } } }
```

### Notes / limitations

- **Completeness:** an `update` is emitted whenever the **outcome** actually
  changes (a changed return value, or a success↔revert flip). The outcome is a
  function only of the accounts it touches, block context, and the fixed call args.
  Every account dependency is watched at **account granularity**, which covers all
  of an account's state — storage, balance, code, and the dependencies no opcode
  exposes: the **key** behind a `validateSender` call, the **code** behind an
  EIP-7702 delegation the call invokes, and the **existence** of a `CREATE`/
  `CREATE2` target. State changes signal via the account watch; block-context calls
  (`trackable=false`, e.g. they read `block.timestamp`/`number`/`basefee`/
  `gasprice`) are **re-evaluated every block** since their result can change with no
  state change. When a re-evaluation discovers a **new** dependency, the engine
  re-evaluates once more on the next block to close the window before that
  dependency joined the watch-list; and a head that **jumps** over blocks (batch
  insert) re-evaluates every subscription. Outcome dedup then emits only on a real
  change — no polling needed, though a block-context subscription costs one
  re-evaluation per block (the price of tracking a value that can drift every
  block). (Assumes IBFT immediate finality — no reorg machinery.)
- **Reverts vs. errors:** a **revert** is a normal outcome — emitted via `snapshot`/
  `update` with `error` set (and an empty `result`) — so a success↔revert flip
  reaches you and you can react (e.g. blacklist a failing call). Two reverts with
  different reasons are **not** distinguished; use `kaia_callWithAccessedStorage` to
  inspect a revert reason. The distinct `error`-type message is reserved for an
  **infrastructure** failure (the node could not evaluate the call), so a transient
  node hiccup is not mistaken for the call reverting.
- Errors:
  - `notifications not supported` — called over HTTP.
  - `callResults: too many active subscriptions (max 1024)` — global cap reached.
- A single evaluation is bounded by `RPCEVMTimeout` and by the node's RPC gas cap
  (or a **50,000,000-gas fallback** when no `--rpc.gascap` is set).

---

## `kaia_callResultSubscriptions`

Lists the active `callResults` subscriptions on the node. Introspection/debugging.

### Parameters

None.

### Returns

`Array<Object>`:

| field | type | description |
|---|---|---|
| `id` | `String` | Subscription id (matches the id returned by `kaia_subscribe`). |
| `lastBlock` | `Quantity` | Block of the last evaluation. |
| `lastResultHash` | `Hash` | Hash of the last outcome (`keccak256(result ‖ error)`); zero until the first evaluation. |
| `depCount` | `Number` | Number of dependency accounts currently watched. |
| `trackable` | `Boolean` | Whether the last evaluation was trackable. |

### Notes

- The result is **node-global**, not per-connection (the JSON-RPC layer does not
  expose a per-connection identity to non-subscription methods). A client matches
  entries to its own subscriptions via `id`. Do not expose this endpoint on an
  untrusted/public RPC.

---

## Managing subscriptions (list & unsubscribe)

Both subscription types (`storageChanges`, `callResults`) use the standard
JSON-RPC pub/sub lifecycle.

### Subscription id

`kaia_subscribe(name, …)` returns a subscription id (hex string). Every
notification for it arrives as a `kaia_subscription` message whose
`params.subscription` equals that id. **Retain the id** — it is the handle used to
unsubscribe and to correlate notifications.

### Unsubscribe

Send `kaia_unsubscribe(<id>)` on the **same connection** that created the
subscription. Returns `true` if the subscription existed and was removed. This is
the built-in RPC mechanism and works under whichever namespace you subscribed
(`kaia`/`klay`/`eth`).

```json
{ "jsonrpc":"2.0", "id":2, "method":"kaia_unsubscribe", "params":["0x4f1…"] }
// → { "jsonrpc":"2.0", "id":2, "result": true }
```

**Closing the WebSocket/IPC connection automatically unsubscribes everything on
it** — server-side resources (the per-subscription goroutine and watch-list
entries) are released. A cleanly disconnected client leaks nothing. A client that
**hangs** (connection alive but not reading) is also released: a notification write
that exceeds the write deadline (default 10s) tears the subscription down. For a
client that is stuck but neither reading nor closing the socket, enable the
WebSocket **read** deadline (`--wsreaddeadline`, disabled by default) so the
connection is detected and closed; the per-node `callResults` cap (1024) bounds the
blast radius regardless.

### Listing

- **`callResults`:** [`kaia_callResultSubscriptions()`](#kaia_callresultsubscriptions)
  returns all active reactive calls on the node (`id`, `lastBlock`,
  `lastResultHash`, `depCount`, `trackable`). Node-global; match entries to your
  own subscriptions by `id`.
- **`storageChanges`:** no server-side list method — track the ids returned by
  `kaia_subscribe` client-side (the standard pub/sub model; the subscribe response
  is your handle).

In all cases the client already holds its ids from the subscribe responses, so a
server-side list is an introspection/ops convenience rather than a requirement.

---

## Shared types

### CallArgs

The standard `kaia_call`/`eth_call` argument object. Common fields:

| field | type | description |
|---|---|---|
| `from` | `Address` | Sender (optional). For `callResults`, the sender's balance/nonce are watched. |
| `to` | `Address` | Target contract. For `callResults`, the recipient is watched. |
| `gas` | `Quantity` | Gas limit (optional; defaults to the upper bound). |
| `gasPrice` / `maxFeePerGas` / `maxPriorityFeePerGas` | `Quantity` | Gas pricing (optional). |
| `value` | `Quantity` | Value in peb (optional). |
| `data` / `input` | `Data` | Call data. |
| `accessList` | `Array<AccessTuple>` | Optional EIP-2930 access list. |
| `chainId` | `Quantity` | Optional. |

### AccessTuple

```json
{ "address": "0x…", "storageKeys": ["0x…", "0x…"] }
```

### Value encodings

- `Address` — 20-byte hex string (`0x…`).
- `Hash` — 32-byte hex string (`0x…`).
- `Data` — variable-length hex string (`0x…`).
- `Quantity` — hex-encoded integer (`0x…`).

---

## Operational notes

- **Transports:** subscriptions require WS/IPC. `unsubscribe` via
  `kaia_unsubscribe(<id>)`; notifications arrive as `kaia_subscription`.
- **Limits:** at most **1024** concurrent `callResults` subscriptions node-wide;
  concurrent EVM evaluations are bounded by a CPU-count semaphore so reactive load
  cannot stall block processing.
- **Finality:** delivery is tied to the canonical `ChainHeadEvent`; the design
  assumes Kaia's immediate finality (no multi-block reorgs) and emits no
  rollback events.
- **Sequence numbers** (`seq`): for `storageChanges`, `seq` may gap on a slow
  connection (a dropped delta) — on a gap, resync via
  `debug_diffContractStorageHash`. For `callResults`, `seq` is contiguous: a send
  dropped while your buffer is full is re-attempted on the next block, so you
  converge to the current result without resyncing.
