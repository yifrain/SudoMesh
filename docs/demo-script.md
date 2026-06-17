# SwarmSolve — Oral Exam Demo Script (English)

A ready-to-run script for the ~10–12 min live demo + Q&A. Simplified-Chinese
version: [`demo-script.zh-CN.md`](demo-script.zh-CN.md).

Legend for each step:
**[Say]** talking points · **[Run]** command · **[Expect]** what to point at ·
**[Highlight]** the course concept to name out loud.

---

## 0. Pre-demo checklist (do this BEFORE you present)

```bash
uv sync --extra dev                 # install deps into .venv
uv run pytest -q                    # sanity: 8 passed
# pre-generate the wide puzzle used in the speedup part:
uv run swarmsolve gen --size 9 --clue-ratio 0.28 --seed 7 --out wide.txt
```
* Make sure ports 9000–9003 are free (the swarm uses them).
* Have two things open: a terminal, and `docs/architecture.en.md` (for the
  architecture diagram and the speedup chart) on a second screen.
* Keep `node-delay` values as written — they are tuned so each part finishes in
  seconds while still showing the effect.

## Timeline

| # | Part | Owner | Time |
|---|------|-------|------|
| 0 | Pitch + architecture | E (lead) | 1.5 min |
| 1 | Solver baseline | D | 1 min |
| 2 | Real P2P swarm + correctness | C | 1.5 min |
| 3 | Fault tolerance | E | 2 min |
| 4 | Live dashboard | C | 1 min |
| 5 | Honest speedup + scaling | B | 2.5 min |
| 6 | Design trade-offs | A | 1.5 min |
| 7 | Q&A | all | rest |

---

## Part 0 — Pitch + architecture  *(owner: E)*

**[Say]** "SwarmSolve solves very large Sudoku (up to 25×25) on a decentralized
P2P network — no central server. The puzzle is a huge search tree; we split it
into subtasks and many equal peers explore different parts in parallel, exchanging
three messages: **Open Task**, **Dead End**, **Solution**."

**[Show]** the 5-layer diagram in `docs/architecture.en.md` §1. Name the layers
bottom-up and the one-line course mapping:

> Transport (Ch.2) → **Discovery: Kademlia DHT (Ch.6)** → Gossip (Ch.2 + Ch.7) →
> Tasks: split/dedup/lease (Ch.5) → Solver.

**[Highlight]** the headline idea in one sentence: *"We reuse the Kademlia XOR
keyspace as the task-ID space, so the DHT doubles as a load balancer — that ties
Ch.6 directly to Ch.5."*

---

## Part 1 — Solver baseline  *(owner: D)*

**[Say]** "First, a single machine. The solver uses bitmask constraint
propagation + MRV depth-first search."

**[Run]**
```bash
uv run swarmsolve solve examples/puzzles/hard_9x9.txt
```
**[Expect]** `Solved!`, the grid, and a line like `time=0.1s nodes=6050`.

**[Highlight]** "6050 nodes is the work one machine does. Remember this — the
swarm will split exactly this kind of work across peers."

---

## Part 2 — Real P2P swarm + correctness  *(owner: C)*

**[Say]** "Now the real thing: four **separate OS processes** talking over real
localhost TCP/UDP. One peer submits; all peers self-discover via Kademlia,
gossip Open Tasks, claim them with leases, share Dead Ends, and the first
Solution stops everyone."

**[Run]**
```bash
uv run swarmsolve demo --file examples/puzzles/hard_9x9.txt --peers 4
```
**[Expect]** the per-peer table, then `Swarm solved!` and the grid.

**[Highlight]** "These are real processes and real sockets — not threads. Point
at the per-peer `nodes`/`tasks_done` columns: the work was genuinely distributed.
Each peer found the solution because it propagates over gossip."

> Honesty note (say it proactively): "On a tiny 9×9 the swarm isn't faster than
> one machine — coordination costs more than the search. The real speedup shows
> on exhaustive search, which I'll demo in Part 5."

---

## Part 3 — Fault tolerance  *(owner: E)*  ⭐ key

**[Say]** "Peers crash. We use **time-boxed leases**: when a peer claims a task
it holds a lease; if it dies, the lease expires and another peer reclaims the
task. I'll run in exhaustive mode so *every* task must finish — meaning the run
can only complete if reassignment actually works. Then I kill a peer mid-solve."

**[Run]**
```bash
uv run swarmsolve fault --file examples/puzzles/hard_9x9.txt \
    --peers 4 --kill-peer 2 --kill-after 1.5 --lease 1.5 --node-delay 0.0008 --split-depth 4
```
**[Expect]**
```
>>> killed peer #2 (PID …)
   killed peer #2 returned a result: no (as expected)
   surviving peers that finished: [0, 1, 3]
   swarm STILL solved the puzzle in ~13s despite the failure
```
**[Highlight]** "Peer 2 never returns — yet the swarm still finishes, because its
leased task was reclaimed by a survivor. This is **fault tolerance via leases +
`reclaim_expired`**, plus Kademlia's churn tolerance (k-buckets route around dead
contacts)."

---

## Part 4 — Live dashboard  *(owner: C)*

**[Say]** "To make the swarm observable, each peer reports a snapshot and we
render a live table."

**[Run]**
```bash
uv run swarmsolve dashboard --file examples/puzzles/hard_9x9.txt --peers 4 --node-delay 0.003
```
**[Expect]** a live `rich` table (per-peer neighbors / open / claimed / dead /
done / nodes / found), then the final report + solved grid.

**[Highlight]** "You can watch tasks move between the open/claimed/done columns
and dead-ends grow as pruning propagates — the protocol in motion."

---

## Part 5 — Honest speedup + scaling  *(owner: B)*  ⭐ key

**[Say]** "First-solution search barely parallelizes — the answer sits on one
deep DFS path. The genuinely parallel workload is **exhaustive search**: count
all solutions / prove uniqueness. We run it in **exact mode**: each task has a
single owner (XOR-closest peer), delivered reliably over TCP, with **virtual
nodes** to balance load — so zero duplicate work and exact solution counts."

**[Run]** (single 4-peer run first)
```bash
uv run swarmsolve benchmark --file wide.txt --peers 4 --node-delay 0.0002
```
**[Expect]** balanced per-peer node counts (~24k each), then:
```
correctness OK: all 45475 solution(s) covered exactly once
speedup  : 2.55x (wall clock)
```
**[Say]** "And it scales — here are 1, 2, 4 peers:"

**[Run]** (optional if time allows)
```bash
for p in 1 2 4; do uv run swarmsolve benchmark --file wide.txt --peers $p --node-delay 0.0002; done
```
**[Show]** the speedup chart in `docs/architecture.en.md` §14:

| peers | wall | vs baseline | vs 1-peer |
|-------|------|-------------|-----------|
| 1 | 34.8 s | 0.80× | 1.0× |
| 2 | 20.3 s | 1.50× | 1.71× |
| 4 | 11.6 s | 2.55× | **3.0×** |

**[Highlight]** "Relative to one peer we get ~3× on 4 peers — **75 % parallel
efficiency** — with **exact counts and ~0 % duplicate work**. Virtual nodes
(consistent hashing, Ch.5) are what made the load even — without them one peer
was doing 47 % of the work."

---

## Part 6 — Design trade-offs  *(owner: A)*

**[Say]** "We support two execution modes, and the contrast is the interesting
part:"

| Mode | Duplicate work | Counts | Best on | Robustness |
|------|----------------|--------|---------|-----------|
| Work-stealing | yes (node-level) | exact (dedup) | unbalanced trees | tolerant of loss/churn |
| Exclusive (default) | none | exact | balanced trees | needs reliable delivery |

**[Highlight]** "This is a clean **consistency-vs-availability** trade-off:
exclusive favours exactness and zero duplication; work-stealing favours
availability and throughput under churn. Two other deliberate choices:
**Kademlia over UDP for discovery, TCP for tasks** (matches the brief's 'TCP
messages' for reliable payloads), and **depth-bounded dead-end sharing** so a
hard puzzle doesn't flood the network with 10k+ messages."

---

## Course-concept callouts (drop these naturally)

* **Ch.2** — gossip + TTL + seen-set de-dup (fixes Gnutella's flooding).
* **Ch.5** — load balancing via uniform XOR placement + virtual nodes.
* **Ch.6** — Kademlia: XOR metric, k-buckets, iterative FIND_NODE.
* **Ch.7** — BubbleStorm-style probabilistic coverage (gossip fan-out reaches all w.h.p.).

---

## Anticipated Q&A

**Q: Why is the 9×9 swarm slower than one machine?**
A: First-solution search is inherently serial along the solution path; for tiny
trees coordination dominates. The parallel win is in exhaustive search — shown in
Part 5 (2.55× on 4 peers).

**Q: How do you avoid two peers doing the same work?**
A: Two layers. Tasks are deduplicated by ID in the scheduler and by a last-moment
check before claiming. In exact mode each task has exactly one XOR-closest owner,
delivered reliably over TCP, so duplication is ~0 % (we verified: node totals
match the baseline and solution counts are exact).

**Q: What happens when a peer joins or leaves?**
A: Join: Kademlia bootstrap + FIND_NODE populates buckets. Leave/crash: its task
leases expire and survivors reclaim them (Part 3). Ownership shifts smoothly
because it's keyed on XOR distance.

**Q: Is it really decentralized? You pass a roster in exact mode.**
A: The roster is a convenience for a clean localhost benchmark; the same
ownership can be computed from the Kademlia routing table via
`is_responsible_for` (the default when no roster is given). Discovery, gossip and
leasing are fully decentralized.

**Q: Why Kademlia and not Chord/CAN?**
A: Kademlia's XOR metric is symmetric and lets one keyspace serve both routing
and task placement; k-buckets prefer long-lived peers (eclipse resistance); it's
UDP-light and churn-tolerant — exactly how it's used in BitTorrent/IPFS.

**Q: Does it scale to 25×25?**
A: The solver is size-generic (N=k²); `gen --size 25` builds one instantly and
high-clue instances solve immediately via propagation. Sparse 25×25 first-solution
is NP-hard (the tree explodes) — that's a property of Sudoku, not our system; the
distributed speedup is quantified on the exhaustive benchmark.

**Q: What would you do next?**
A: Churn-aware ownership without a roster, adaptive work-donation for unbalanced
trees, and the jigsaw extension (swap only the `solver/` package).

---

## If a command hangs or a port is busy

* Ctrl-C, then re-run; ports free within a second or two.
* Reduce `--peers` to 3, or bump `--node-delay` down 2× if a machine is slow.
* Fallback talking point: the unit tests (`uv run pytest -q`) prove solver +
  DHT correctness without any networking.
