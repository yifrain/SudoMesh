# SwarmSolve — Detailed Architecture (English)

This document is a **code-level** walkthrough of SwarmSolve. For the high-level
pitch, quickstart and team split see the root [`README.md`](../README.md).

---

## 1. Layered architecture

```
┌──────────────────────────────────────────────────────────────┐
│ Peer  (one OS process / one machine)                          │
│                                                                │
│  Solver      constraint propagation + DFS        solver/       │
│  Task        split / dedup / lease / rebalance   tasks/        │
│  Gossip      epidemic spread + de-dup            gossip/       │
│  Discovery   Kademlia DHT (XOR, k-buckets)       discovery/    │
│  Transport   TCP (tasks) + UDP (discovery)       transport/    │
└──────────────────────────────────────────────────────────────┘
        ▲                                                  ▲
        └──────────── TCP / UDP over localhost/LAN ────────┘
```

| Layer | File(s) | Course concept |
|-------|---------|----------------|
| Transport | [`transport/messages.py`](../src/swarmsolve/transport/messages.py), [`transport/transport.py`](../src/swarmsolve/transport/transport.py) | Ch.2 — TCP, messaging |
| Discovery | [`discovery/node_id.py`](../src/swarmsolve/discovery/node_id.py), [`routing.py`](../src/swarmsolve/discovery/routing.py), [`kademlia.py`](../src/swarmsolve/discovery/kademlia.py) | **Ch.6 — Kademlia** |
| Gossip | [`gossip/gossip.py`](../src/swarmsolve/gossip/gossip.py) | Ch.2 Gossip + Ch.7 BubbleStorm |
| Task | [`tasks/task.py`](../src/swarmsolve/tasks/task.py), [`scheduler.py`](../src/swarmsolve/tasks/scheduler.py) | Ch.5 load balancing + fault tolerance |
| Solver | [`solver/board.py`](../src/swarmsolve/solver/board.py), [`search.py`](../src/swarmsolve/solver/search.py) | application core |
| Orchestration | [`peer.py`](../src/swarmsolve/peer.py), [`cli.py`](../src/swarmsolve/cli.py) | glue + demos |

---

## 2. Repository layout

```
src/swarmsolve/
├── transport/
│   ├── messages.py   # Message dataclass + MessageType enum + encode/decode
│   └── transport.py  # asyncio TCP server + UDP endpoint
├── discovery/
│   ├── node_id.py    # 160-bit NodeID, XOR distance, task_key()
│   ├── routing.py    # k-bucket RoutingTable, Contact
│   └── kademlia.py   # PING/PONG/FIND_NODE, iterative lookup, bootstrap
├── gossip/
│   └── gossip.py     # push gossip: seen-set de-dup + TTL fan-out
├── tasks/
│   ├── task.py       # Task (a subtree) + TaskStatus + path_repr/task_key
│   └── scheduler.py  # open/claimed/dead/done sets, leases, MRV-by-XOR pick
├── solver/
│   ├── board.py      # bitmask board + constraint propagation
│   └── search.py     # DFS, subtree split, enumerate, node_delay
├── puzzles.py        # parse / generate puzzles (instant full-grid construction)
├── peer.py           # Peer: wires every layer + the work loop
└── cli.py            # gen / solve / demo / benchmark / dashboard / fault / peer
```

---

## 3. Module-by-module walkthrough

### 3.1 Transport — `transport/`

**`messages.py`** defines the single wire type [`Message`](../src/swarmsolve/transport/messages.py)
(`type, sender, payload, msg_id, ttl, ts`) and the [`MessageType`](../src/swarmsolve/transport/messages.py)
enum. The three *application* types required by the brief are `OPEN_TASK`,
`DEAD_END`, `SOLUTION`; the rest are coordination (`TASK_CLAIM`, `TASK_DONE`) and
discovery (`PING/PONG/FIND_NODE/FIND_NODE_REPLY`). Serialization is
newline-delimited JSON (`encode`/`decode`) — readable for the demo, swappable
for msgpack later.

**`transport.py`** — the [`Transport`](../src/swarmsolve/transport/transport.py)
class owns a `asyncio` **TCP server** (one request per connection, used for
task/solution payloads) and a **UDP endpoint** (datagrams, used for Kademlia).
A single `handler(msg, addr, kind)` callback receives everything; `send_tcp`
returns `False` if a peer is down (used to detect failures), `send_udp` is
best-effort.

### 3.2 Discovery (Kademlia) — `discovery/`

**`node_id.py`** — [`NodeID`](../src/swarmsolve/discovery/node_id.py) is a 160-bit
ID. Key functions: `xor_distance`, `shared_prefix_len` (the bucket index), and
crucially [`task_key`](../src/swarmsolve/discovery/node_id.py) which hashes a
search-tree path into the **same** XOR space as node IDs — the bridge between the
Solver and the DHT.

**`routing.py`** — [`RoutingTable`](../src/swarmsolve/discovery/routing.py) holds
`ID_BITS` k-buckets (`K=8`). `add` is LRU within a bucket (prefers long-lived
peers → eclipse resistance); `closest(target, n)` returns the n nearest contacts
by XOR distance — used both for routing and for task ownership.

**`kademlia.py`** — [`KademliaNode`](../src/swarmsolve/discovery/kademlia.py)
implements `PING/PONG`, `FIND_NODE`, iterative `lookup` (O(log n) rounds),
`bootstrap`, and `is_responsible_for(key, replicas)` (am I among the closest
peers to this key?). STORE/FIND_VALUE are intentionally omitted — we only use the
keyspace for *routing tasks to their closest peers*, not for value storage.

### 3.3 Gossip — `gossip/`

[`Gossip`](../src/swarmsolve/gossip/gossip.py) is push-based: on receipt it (1)
drops duplicates via a bounded `seen` `OrderedDict`, (2) delivers to the local
`deliver` callback, (3) if `ttl > 0`, decrements and forwards to a random
`fanout` (=3) subset of neighbours. This bounds traffic while still reaching the
overlay with high probability (the BubbleStorm intuition, Ch.7).

### 3.4 Task layer — `tasks/`

**`task.py`** — a [`Task`](../src/swarmsolve/tasks/task.py) is a subtree of the
search space, identified by its assignment `path`. `path_repr` is the canonical
(order-independent) string; `Task.key` is `task_key(path_repr)` → its position in
the XOR space. `lease_active()` tells whether a claim is still valid.

**`scheduler.py`** — [`Scheduler`](../src/swarmsolve/tasks/scheduler.py) is the
per-peer brain. State: `open`, `claimed`, `dead_ends`, `done`. Highlights:

* `add_open` ignores tasks already dead/done/actively-claimed (dedup).
* `next_task` picks the open task with the **smallest XOR distance to my ID** —
  this is the structured, low-collision placement (Ch.5/Ch.6).
* `reclaim_expired` moves expired leases back to `open` → automatic reassignment
  when a peer crashes (fault tolerance).

### 3.5 Solver — `solver/`

**`board.py`** — [`Board`](../src/swarmsolve/solver/board.py) stores one **bitmask
of candidates per cell**. `assign` does elimination + naked-singles propagation
(AC-3-style) and raises `Contradiction` on conflict. `most_constrained_cell`
implements the MRV heuristic. Works for any N=k² (9/16/25).

**`search.py`** — three primitives:
* [`expand_subtasks`](../src/swarmsolve/solver/search.py) — split a node into one
  child path per candidate of its MRV cell (pruning immediate contradictions).
* [`solve_subtree`](../src/swarmsolve/solver/search.py) — DFS one subtree, with
  hooks `is_dead_end` / `record_dead_end` / `should_stop`, plus `node_delay`
  (demo cost knob) and `enumerate_all` (explore whole tree / count solutions).
* [`solve_local`](../src/swarmsolve/solver/search.py) — single-machine baseline.

### 3.6 Orchestration — `peer.py`

[`Peer`](../src/swarmsolve/peer.py) wires every layer and runs the work loop. Key
methods: `start`/`bootstrap`, `_dispatch` (route discovery vs gossip), `_on_gossip`
(apply OPEN_TASK/DEAD_END/TASK_DONE/TASK_CLAIM/SOLUTION), `seed_frontier`+`submit`
(producer), `run`+`_work_on` (consumer), and the pruning hooks. Notable flags:
`split_depth` (work-stealing), `enumerate_mode`, `lease_seconds`, `idle_limit`,
`node_delay`, `dead_end_share_depth`, `on_tick` (dashboard).

### 3.7 CLI — `cli.py`

[`cli.py`](../src/swarmsolve/cli.py) exposes the commands and the shared
multi-process machinery (`_peer_worker`, `_spawn`, `_collect`). `_collect` is
robust to killed peers (polls liveness instead of expecting N results).

---

## 4. Message protocol

| Type | Transport | Payload | Purpose |
|------|-----------|---------|---------|
| `PING`/`PONG` | UDP | host, port | liveness / bucket refresh |
| `FIND_NODE` | UDP | target | iterative lookup |
| `FIND_NODE_REPLY` | UDP | target, nodes[], reply_to | lookup answer |
| `OPEN_TASK` | TCP/gossip | task | advertise an unexplored subtree |
| `TASK_CLAIM` | TCP/gossip | task (owner, lease) | "I'm taking this" |
| `DEAD_END` | TCP/gossip | path | prune this subtree everywhere |
| `TASK_DONE` | TCP/gossip | path | subtree fully explored |
| `SOLUTION` | TCP/gossip | board (flat) | final answer → everyone stops |

---

## 5. End-to-end flow

```mermaid
sequenceDiagram
    participant S as Submitter
    participant A as Peer A
    participant B as Peer B
    S->>S: seed_frontier() splits root into subtasks
    S-->>A: OPEN_TASK*
    S-->>B: OPEN_TASK*
    A->>A: next_task() = closest-to-my-ID, claim (lease)
    B->>B: next_task() (different task), claim (lease)
    A->>A: DFS; shallow contradiction
    A-->>B: DEAD_END(path)
    B->>B: prune that subtree
    B->>B: DFS → SOLUTION
    B-->>S: SOLUTION
    B-->>A: SOLUTION
    Note over S,A,B: should_stop() fires everywhere
```

---

## 6. Key mechanisms (deep dive)

* **XOR task placement.** `task_key(path)` lives in the node ID space, so
  `next_task` preferring the task closest to my ID spreads work deterministically
  and with few collisions — Kademlia (Ch.6) doubling as a load balancer (Ch.5).
* **Work-stealing (`split_depth`).** While a task is shallower than `split_depth`,
  `_work_on` re-splits it into finer OPEN_TASKs and gossips them instead of
  solving it itself. The grain adapts to the swarm size so idle peers get work.
* **Leases & reassignment.** `claim_local` sets `lease_expires = now + lease`.
  `reclaim_expired` (called inside `next_task`) returns expired tasks to `open`,
  so a crashed peer's work is redone. `idle_limit` keeps peers alive long enough
  for a lease to lapse.
* **De-duplication.** `add_open` + a last-moment check in `_work_on` skip tasks
  that are already done/dead/actively-claimed, cutting most duplicate work caused
  by gossip latency (the project's "avoid duplicate work" challenge).
* **Dead-end depth bound (`dead_end_share_depth`).** Only *shallow* dead ends are
  gossiped; deep leaf dead ends are too many and too specific. Without this a hard
  puzzle floods the network with 10k+ messages.
* **`node_delay`.** A demo-only artificial per-search-node cost. Real Sudoku nodes
  are too cheap to expose network effects, so this stands in for "expensive" work
  (25×25 / jigsaw) when measuring speedup, recovery and the dashboard.

---

## 7. The three demos

### A) Fault tolerance — `swarmsolve fault`
Runs in **exhaustive mode** with a large `idle_limit`, so every task *must*
complete. It kills one peer mid-solve (`--kill-peer`, `--kill-after`); that
peer's lease (`--lease`) lapses and its task is reclaimed by a survivor. The run
only finishes if reassignment worked.
```bash
uv run swarmsolve fault --file examples/puzzles/hard_9x9.txt \
    --peers 4 --kill-peer 2 --kill-after 1.5 --lease 1.5 --node-delay 0.0008
```
Look for: *"killed peer #2 returned a result: no"* and *"swarm STILL solved …"*.

### C) Live dashboard — `swarmsolve dashboard`
Each peer reports a snapshot via `on_tick`; the parent renders a `rich.Live`
table (neighbours / open / claimed / dead / done / nodes / found per peer).
```bash
uv run swarmsolve dashboard --file examples/puzzles/hard_9x9.txt --peers 4 --node-delay 0.003
```

### B) Real speedup — `swarmsolve benchmark`
The honest speedup story. **First-solution** search (`demo`) puts the answer on
one deep DFS path that can't be parallelized. **Exhaustive** search
(`benchmark` — count all solutions / prove uniqueness) is embarrassingly
parallel and shows near-linear speedup.
```bash
uv run swarmsolve benchmark --file examples/puzzles/hard_9x9.txt \
    --peers 4 --node-delay 0.0012 --split-depth 4
# baseline ~14.5s ; swarm ~8.7s ; speedup ~1.67x ; solutions match
```

---

## 8. Performance — an honest discussion

* For **first-solution** Sudoku, wall-clock speedup is limited: the solution lies
  on one deep path, and DFS along it is inherently serial. Coordination overhead
  can even make a tiny 9×9 *slower* than a single machine.
* For **exhaustive** workloads the speedup is real (≈1.3–1.7× on 4 peers in our
  runs). It is below the ideal 4× because of (a) duplicate exploration from
  asynchronous gossip and (b) load imbalance when one subtree is much bigger than
  others. Deeper `split_depth` improves balance but increases duplicate work — a
  classic distributed-search trade-off and a great discussion point for the
  report.
* Future work to push speedup up: deterministic single-owner execution by XOR key
  (eliminates duplicates), better churn-aware ownership, and finer adaptive
  splitting (work-stealing on demand).

---

## 9. Extension: jigsaw puzzles

The framework is puzzle-agnostic: anything expressible as *"a search tree split
into subtasks + dead-end pruning + first/all solutions"* fits. For a jigsaw, each
**piece placement** is a branch and an invalid partial assembly is a dead end.
Only the `solver/` package changes; transport/discovery/gossip/tasks are reused.

---

## 10. Course concept mapping

| Concept | Where in code |
|---------|---------------|
| Gnutella-style messaging, TTL flooding (Ch.2) | `transport/`, `gossip/` |
| Gossip / epidemic dissemination (Ch.2) | `gossip/gossip.py` |
| Kademlia: XOR metric, k-buckets, FIND_NODE (Ch.6) | `discovery/` |
| Structured placement / load balancing (Ch.5) | `task_key` + `scheduler.next_task` |
| Probabilistic coverage (Ch.7 BubbleStorm) | gossip fan-out + seen-set |
| Fault tolerance / churn | leases + `reclaim_expired` + `is_responsible_for` |
