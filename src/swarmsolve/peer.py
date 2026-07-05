"""Peer orchestration: wire all layers together and run the solve loop.

Lifecycle
---------
1. start transport (TCP+UDP) and register the message dispatcher;
2. join the overlay via Kademlia bootstrap;
3. the *submitter* peer splits the root puzzle into a task frontier and gossips
   OPEN_TASK messages;
4. every peer repeatedly: pick a task it "owns" -> claim (lease) -> either
   explore the subtree, or (work-stealing) re-split a shallow task into finer
   OPEN_TASKs so idle peers get work -> gossip DEAD_END / SOLUTION / TASK_DONE;
5. shared dead-ends prune everyone's search; the first SOLUTION stops the swarm;
6. a crashed peer's lease expires and its task is automatically reassigned.

Owner: Person E (orchestration) — uses every other member's module.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque

from swarmsolve.discovery.kademlia import KademliaNode
from swarmsolve.discovery.node_id import NodeID, task_key, xor_distance
from swarmsolve.discovery.routing import Contact
from swarmsolve.gossip.gossip import Gossip
from swarmsolve.solver.board import Board, Contradiction
from swarmsolve.solver.search import (
    Path,
    apply_path,
    estimate_subtree_size,
    expand_subtasks,
    solve_subtree,
)
from swarmsolve.tasks.guard import (
    CLAIMED,
    DONE_EXHAUSTED,
    DONE_SPLIT,
    OPEN,
    GuardRecord,
    GuardStore,
)
from swarmsolve.tasks.scheduler import Scheduler
from swarmsolve.tasks.task import Task, TaskStatus, path_repr
from swarmsolve.transport.messages import Message, MessageType
from swarmsolve.transport.transport import Transport


class Peer:
    def __init__(
        self,
        host: str,
        port: int,
        board: Board,
        node_id: NodeID | None = None,
        *,
        log=print,
        dead_end_share_depth: int = 3,
        lease_seconds: float = 10.0,
        node_delay: float = 0.0,
        split_depth: int = 0,
        enumerate_mode: bool = False,
        idle_limit: int = 30,
        exclusive: bool = False,
        owner_roster: list[tuple[NodeID, Contact]] | None = None,
        probe_random: bool = False,
        detect_unsolvable: bool = False,
        root_replicas: int = 3,
        steal: bool = False,
        steal_yield_every: int = 64,
        steal_scan: int = 8,
        sync_interval: float = 0.0,
        guard: bool = False,
        guard_k: int = 3,
        max_split_depth: int = 2,
        on_tick=None,
        tick_interval: float = 0.15,
    ) -> None:
        self.host = host
        self.port = port
        self.board = board
        self.id = node_id or NodeID.from_string(f"{host}:{port}")
        self.transport = Transport(host, port)
        self.dht = KademliaNode(self.id, self.transport)
        self.gossip = Gossip(self.transport, self.dht.table)
        self.scheduler = Scheduler(self.id, lease_seconds=lease_seconds)
        self.lease_seconds = lease_seconds
        self.log = log
        # Only *shallow* dead ends are worth sharing: deep leaf dead-ends are
        # too numerous and too specific to help other peers. This keeps gossip
        # traffic bounded (otherwise a hard puzzle floods the network).
        self.dead_end_share_depth = dead_end_share_depth
        self.node_delay = node_delay
        # Work-stealing: while a task is shallower than ``split_depth`` we
        # re-split it into finer OPEN_TASKs instead of solving it ourselves,
        # so the grain adapts to the number of peers and idle peers get work.
        # 0 disables work-stealing (one peer solves each seeded subtree).
        self.split_depth = split_depth
        # Enumerate mode: explore the WHOLE tree (count all solutions / prove
        # uniqueness) instead of stopping at the first solution. This workload
        # is embarrassingly parallel and is what shows near-linear speedup.
        self.enumerate_mode = enumerate_mode
        self.solutions = 0
        # How many idle polls before giving up. Larger values give a crashed
        # peer's lease time to expire so its task can be reclaimed (fault demo).
        self.idle_limit = idle_limit
        # Deterministic single-owner execution. In exclusive mode a task is run
        # ONLY by the peer whose ID is XOR-closest to the task key, eliminating
        # duplicate exploration -> exact solution counts + near-linear speedup.
        # ``owner_roster`` (all peer NodeIDs) gives a consistent ownership view;
        # if None we fall back to the Kademlia routing table (decentralized).
        self.exclusive = exclusive
        self.owner_roster = owner_roster

        # Optimization 1: pull-based discovery. When idle, actively probe with a
        # random NodeID (via Kademlia lookup) and ask the peers it converges on
        # for open tasks (TASK_QUERY/TASK_OFFER), instead of only waiting for a
        # gossip push. Complements, does not replace, the push path.
        self.probe_random = probe_random
        # Optimization 2+3: hierarchical unsolvable detection. When enabled the
        # root puzzle and every subtask carry parent/child bookkeeping; a branch
        # searched to exhaustion reports DONE_EXHAUSTED up the tree until the
        # root becomes exhausted -> the puzzle is proven unsolvable. The root
        # record is replicated to ``root_replicas`` peers to avoid a single
        # point of failure. ``self.unsolvable`` records the verdict.
        self.detect_unsolvable = detect_unsolvable
        self.root_replicas = root_replicas
        self.unsolvable = False

        # Optimization (Chord-style): fine-grained work stealing. In steal mode
        # a busy peer explores its subtree with an explicit, *stealable* deque of
        # frontier paths (owner works the tail LIFO; a thief steals from the head
        # -- the shallowest, coarsest branch). Idle peers steal via the existing
        # TASK_QUERY/TASK_OFFER pull channel, so no branch is duplicated. The DFS
        # yields to the event loop periodically so TASK_QUERY can be served while
        # we compute (asyncio is single-threaded).
        self.steal = steal
        self.steal_yield_every = max(1, steal_yield_every)
        self._steal_deque: "deque[Path] | None" = None
        # Search-space estimation: when a thief steals, scan the head window of
        # the deque and hand out the branch with the largest estimated subtree
        # (heaviest work), so load balances toward where the real work is.
        self.steal_scan = max(1, steal_scan)
        # Periodic state sync: a busy peer snapshots its unexplored frontier to
        # backup peers every ``sync_interval`` seconds (0 = disabled). On owner
        # crash, a backup resumes from the snapshot instead of redoing the whole
        # subtree (only the sync window's work is lost).
        self.sync_interval = sync_interval
        self._sync_last = 0.0
        self._current_task_id: str | None = None

        # Task Guards (Kademlia non-exclusive mode). A task is PUT on its ``k``
        # nearest peers, which track its state and coordinate purely by
        # point-to-point TCP (UPDATE_STATUS etc.). Idle peers become *thieves*:
        # they first self-claim any OPEN task they themselves guard (cold-start
        # Opt A), else random-key lookup the nearest active peer and WORK_STEAL.
        # On split they instant-self-claim one child (Opt B) -> zero idle time.
        # Only the final SOLUTION / unsolvable verdict is gossiped globally.
        self.guard = guard
        self.guard_k = guard_k
        self.max_split_depth = max_split_depth
        self.guards = GuardStore(lease_seconds=lease_seconds)
        self._guard_hb_last = 0.0
        self._guard_maint_last = 0.0
        self.steals_ok = 0
        self.steals_fail = 0
        self._pending_steal: dict[str, asyncio.Future] = {}

        # Observability hook (used by the live dashboard).
        self.on_tick = on_tick
        self.tick_interval = tick_interval
        self._last_tick = 0.0

        self.solution: Board | None = None
        self._stop = asyncio.Event()
        self.nodes_expanded = 0
        self.tasks_done = 0
        self._bootstrap_contacts: list[Contact] = []

    # ---- lifecycle ----------------------------------------------------

    async def start(self, bootstrap: list[Contact] | None = None) -> None:
        await self.transport.start(self._dispatch)
        self.gossip.deliver = self._on_gossip
        self._bootstrap_contacts = bootstrap or []
        if bootstrap:
            await self.dht.bootstrap(bootstrap)
        self.log(f"[{self.id.short()}] up on {self.host}:{self.port} "
                 f"(peers={self.dht.table.size()})")

    async def stop(self) -> None:
        self._stop.set()
        await self.transport.stop()

    def contact(self) -> Contact:
        return Contact(self.id, self.host, self.port)

    # ---- observability ------------------------------------------------

    def snapshot(self) -> dict:
        """Current state for the dashboard / reports."""
        s = self.scheduler.stats()
        s.update(
            {
                "id": self.id.short(),
                "peers": self.dht.table.size(),
                "nodes": self.nodes_expanded,
                "tasks_done": self.tasks_done,
                "solutions": self.solutions,
                "found": self.solution is not None,
                "unsolvable": self.unsolvable,
            }
        )
        if self.guard:
            s.update(self.guards.stats())
            s["steals_ok"] = self.steals_ok
            s["steals_fail"] = self.steals_fail
        return s

    def _maybe_tick(self) -> None:
        if not self.on_tick:
            return
        now = time.monotonic()
        if now - self._last_tick >= self.tick_interval:
            self._last_tick = now
            self.on_tick(self.snapshot())

    # ---- message dispatch ---------------------------------------------

    async def _dispatch(self, msg: Message, addr, kind: str) -> None:
        """Single inbound entry point for EVERY message this peer receives.

        Routes by category: Kademlia RPCs go straight to the DHT; all
        application + coordination traffic goes through gossip, which de-dups,
        delivers locally (``_on_gossip``) and re-forwards to neighbours.
        """
        if msg.type in (
            MessageType.PING,
            MessageType.PONG,
            MessageType.FIND_NODE,
            MessageType.FIND_NODE_REPLY,
        ):
            await self.dht.handle(msg, addr)
        elif msg.type in (
            MessageType.WORK_STEAL,
            MessageType.UPDATE_STATUS,
            MessageType.REPORT_SPLIT,
            MessageType.REPORT_EXHAUSTED,
            MessageType.REPORT_CHILD_EXHAUSTED,
            MessageType.HEARTBEAT,
            MessageType.TASK_OFFER,
        ) and self.guard:
            # Task-Guards point-to-point coordination (never gossip re-forwarded).
            await self._on_guard_msg(msg, addr)
        elif msg.type in (MessageType.TASK_QUERY, MessageType.TASK_OFFER):
            # point-to-point pull (not gossip): request/response, no re-forward
            await self._handle_pull(msg)
        elif msg.type == MessageType.STATE_SYNC:
            # point-to-point backup snapshot from a busy owner
            self._on_state_sync(msg)
        else:
            await self.gossip.handle(msg)

    async def _on_gossip(self, msg: Message) -> None:
        """Receiving end of every application link: apply one delivered message
        to local state. Each branch is one of the protocol's data-flow links."""
        if msg.type == MessageType.OPEN_TASK:
            # producer link: a new unexplored subtree we may pick up
            self.scheduler.add_open(Task.from_dict(msg.payload["task"]))
        elif msg.type == MessageType.DEAD_END:
            # pruning link: this subtree is invalid -> never explore it again
            self.scheduler.mark_dead([tuple(p) for p in msg.payload["path"]])
        elif msg.type == MessageType.TASK_DONE:
            # progress link: subtree already fully explored elsewhere
            self.scheduler.mark_done([tuple(p) for p in msg.payload["path"]])
        elif msg.type == MessageType.TASK_CLAIM:
            # dedup link: someone leased this task -> drop it from our open pool
            self.scheduler.note_claim(Task.from_dict(msg.payload["task"]))
        elif msg.type == MessageType.SPLIT_REPORT:
            # unsolvable link: a task was expanded into children; record the
            # parent bookkeeping so we can later count exhausted children.
            if self.detect_unsolvable:
                self.scheduler.mark_split(Task.from_dict(msg.payload["task"]))
        elif msg.type == MessageType.EXHAUSTED_REPORT:
            # unsolvable link: a child branch was exhausted -> tally it up.
            if self.detect_unsolvable:
                await self._on_exhausted_report(
                    msg.payload["parent_id"], msg.payload["child_id"]
                )
        elif msg.type == MessageType.SOLUTION:
            # termination link: the first solution stops the whole swarm
            if msg.payload.get("unsolvable"):
                # unsolvable verdict fan-out: agree and stop the whole swarm
                if not self.unsolvable:
                    self.unsolvable = True
                    self.log(f"[{self.id.short()}] received UNSOLVABLE verdict")
                    self._stop.set()
                    relay = Message(
                        MessageType.SOLUTION, self.id.hex(),
                        msg.payload, msg_id=msg.msg_id, ttl=3, ts=msg.ts,
                    )
                    for c in self.dht.table.all_contacts():
                        await self.transport.send_tcp(c.host, c.port, relay)
            elif self.solution is None:
                flat = msg.payload["board"]
                self.solution = Board.from_flat(self.board.n, flat)
                self.log(f"[{self.id.short()}] received SOLUTION via gossip")
                self._stop.set()
                # Forward to ALL known peers so everyone stops quickly
                relay = Message(
                    MessageType.SOLUTION, self.id.hex(),
                    msg.payload, msg_id=msg.msg_id, ttl=3, ts=msg.ts,
                )
                for c in self.dht.table.all_contacts():
                    await self.transport.send_tcp(c.host, c.port, relay)

    # ---- task production (submitter only) -----------------------------

    def seed_frontier(self, target: int) -> list[Task]:
        """BFS-expand the root until we have ~``target`` open subtasks."""
        frontier: list[Path] = [[]]
        while len(frontier) < target:
            grown: list[Path] = []
            progressed = False
            for path in frontier:
                children = expand_subtasks(self.board, path)
                if children:
                    grown.extend(children)
                    progressed = True
                else:
                    grown.append(path)
            frontier = grown
            if not progressed:
                break
        return [Task(path=p) for p in frontier]

    async def submit(self, target_tasks: int) -> None:
        if self.guard:
            await self._guard_submit_root()
            return
        if self.detect_unsolvable:
            await self._submit_root()
            return
        tasks = self.seed_frontier(target_tasks)
        self.log(f"[{self.id.short()}] seeding {len(tasks)} open tasks")
        for t in tasks:
            await self._route_open_task(t)

    async def _submit_root(self) -> None:
        """Submit the root puzzle as a replicated, tree-tracked task.

        Optimization 2+3: the root itself is an ordinary task (``path=[]``,
        ``parent_id=None``). We expand it *one* level into direct children,
        record the root as DONE_SPLIT, replicate that record to peers (gossip),
        and publish the children. From here the tree grows recursively as peers
        claim and split children, and DONE_EXHAUSTED reports flow back up.
        """
        children_paths = expand_subtasks(self.board, [])
        root = Task(path=[], parent_id=None)
        if not children_paths:
            # Root cannot branch: either already solved, or immediately
            # contradictory. Decide locally.
            result = solve_subtree(self.board, [], enumerate_all=True)
            if result.stats.solutions > 0 and result.board is not None:
                self.solution = result.board
                self.solutions += result.stats.solutions
                await self.gossip.broadcast(
                    Message(MessageType.SOLUTION, self.id.hex(),
                            {"board": result.board.to_flat()})
                )
            else:
                await self._declare_unsolvable()
            self._stop.set()
            return
        child_tasks = [Task(path=p, parent_id=root.id) for p in children_paths]
        root.children = [t.id for t in child_tasks]
        root.status = TaskStatus.DONE_SPLIT
        self.scheduler.mark_split(root)
        await self._replicate_split(root)
        self.log(f"[{self.id.short()}] seeding root -> {len(child_tasks)} "
                 f"children (unsolvable-detection on)")
        for t in child_tasks:
            await self._route_open_task(t)

    # ---- pull-based discovery (Optimization 1) ------------------------

    async def _handle_pull(self, msg: Message) -> None:
        """Answer/consume a random-id probe (cold-start help + work stealing)."""
        if msg.type == MessageType.TASK_QUERY:
            task = self._steal_from_deque() or self.scheduler.offer_open_task()
            if task is None:
                return
            host = msg.payload.get("host")
            port = msg.payload.get("port")
            if host and port:
                await self.transport.send_tcp(
                    host, int(port),
                    Message(MessageType.TASK_OFFER, self.id.hex(),
                            {"task": task.to_dict()}),
                )
        elif msg.type == MessageType.TASK_OFFER:
            self.scheduler.add_open(Task.from_dict(msg.payload["task"]))

    def _steal_from_deque(self) -> Task | None:
        """Give a thief the *heaviest* unexplored branch we hold.

        Scans the head window (``steal_scan`` shallowest branches — the owner
        works the tail, so the head is coarsest) and hands out the one with the
        largest estimated subtree (search-space estimation). The stolen branch is
        removed from our deque, so no work is duplicated. We keep at least one
        branch for ourselves so we don't go idle.
        """
        if not self._steal_deque or len(self._steal_deque) <= 1:
            return None
        window = min(self.steal_scan, len(self._steal_deque) - 1)
        # candidates: the first ``window`` (shallowest) branches
        best_i = 0
        best_score = -1.0
        for i in range(window):
            score = estimate_subtree_size(self.board, self._steal_deque[i])
            if score > best_score:
                best_score = score
                best_i = i
        stolen_path = self._steal_deque[best_i]
        del self._steal_deque[best_i]
        return Task(path=list(stolen_path))

    async def _probe_for_tasks(self) -> None:
        """Optimization 1: probe a random point of the keyspace for open tasks.

        Different idle peers probe different random keys, spreading the load and
        avoiding a stampede onto one 'hot' owner.
        """
        target = NodeID.random()
        try:
            contacts = await self.dht.lookup(target)
        except Exception:
            contacts = self.dht.table.closest(target)
        query = Message(MessageType.TASK_QUERY, self.id.hex(),
                        {"host": self.host, "port": self.port})
        for c in contacts[:3]:
            if c.node_id != self.id:
                await self.transport.send_tcp(c.host, c.port, query)

    # ---- periodic state sync (crash recovery) -------------------------

    async def _sync_state(self, task_id: str, frontier: list[Path]) -> None:
        """Snapshot our unexplored frontier to the task's backup peers.

        Backups are the peers XOR-closest to the task key (excluding us). On our
        crash, a backup resumes from this snapshot instead of redoing the whole
        subtree — only the last sync window's progress is lost.
        """
        if not frontier:
            return
        contacts = self.dht.closest_to_key(task_key(task_id), self.root_replicas)
        payload = {"task_id": task_id, "frontier": frontier,
                   "nodes": self.nodes_expanded}
        msg = Message(MessageType.STATE_SYNC, self.id.hex(), payload)
        for c in contacts:
            if c.node_id != self.id:
                await self.transport.send_tcp(c.host, c.port, msg)

    def _on_state_sync(self, msg: Message) -> None:
        """Store a busy owner's frontier snapshot for possible crash recovery."""
        p = msg.payload
        self.scheduler.record_backup(
            p["task_id"],
            [[tuple(a) for a in path] for path in p.get("frontier", [])],
            int(p.get("nodes", 0)),
        )

    # ---- unsolvable aggregation (Optimization 2) ----------------------

    async def _replicate_split(self, task: Task) -> None:
        """Broadcast a DONE_SPLIT record so the task's owners can tally children.

        Uses gossip so the record reaches the peers responsible for the task
        (and, at demo scale, everyone) -> robust to a single owner crashing.
        """
        await self.gossip.broadcast(
            Message(MessageType.SPLIT_REPORT, self.id.hex(),
                    {"task": task.to_dict()})
        )

    async def _report_exhausted(self, task: Task) -> None:
        """A branch has no solution: report DONE_EXHAUSTED up to its parent."""
        if task.parent_id is None:
            # This exhausted task IS the root -> the puzzle is unsolvable.
            await self._declare_unsolvable()
            return
        await self.gossip.broadcast(
            Message(MessageType.EXHAUSTED_REPORT, self.id.hex(),
                    {"parent_id": task.parent_id, "child_id": task.id})
        )
        # Apply locally too (gossip.broadcast does not deliver back to us).
        await self._on_exhausted_report(task.parent_id, task.id)

    async def _on_exhausted_report(self, parent_id: str, child_id: str) -> None:
        """Tally a child's exhaustion; roll up / declare unsolvable if complete."""
        all_done = self.scheduler.note_child_exhausted(parent_id, child_id)
        if not all_done or parent_id in self.scheduler.exhausted:
            return
        self.scheduler.exhausted.add(parent_id)
        parent = self.scheduler.split.get(parent_id)
        if parent is None:
            return
        if parent.parent_id is None:
            await self._declare_unsolvable()
        else:
            # propagate one level up (parent now acts as an exhausted child)
            await self.gossip.broadcast(
                Message(MessageType.EXHAUSTED_REPORT, self.id.hex(),
                        {"parent_id": parent.parent_id, "child_id": parent_id})
            )
            await self._on_exhausted_report(parent.parent_id, parent_id)

    async def _declare_unsolvable(self) -> None:
        if self.unsolvable:
            return
        self.unsolvable = True
        self.log(f"[{self.id.short()}] UNSOLVABLE: root exhausted, "
                 f"puzzle has no solution")
        # Fan out the verdict FIRST (awaited, before we set _stop and shut down)
        # so the whole swarm agrees and stops promptly. Reuses the SOLUTION
        # termination link with an ``unsolvable`` sentinel.
        await self.gossip.broadcast(
            Message(MessageType.SOLUTION, self.id.hex(), {"unsolvable": True})
        )
        self._stop.set()

    # ================================================================== #
    # Task Guards (Kademlia non-exclusive mode)
    # ================================================================== #

    def _guard_set(self, key: NodeID) -> list[Contact]:
        """The ``k`` peers closest to ``key`` (guards), *including ourselves*."""
        cand = list(self.dht.table.all_contacts()) + [self.contact()]
        cand.sort(key=lambda c: xor_distance(c.node_id, key))
        return cand[: self.guard_k]

    def _i_am_guard(self, key: NodeID) -> bool:
        return any(c.node_id == self.id for c in self._guard_set(key))

    def _is_primary_guard(self, key: NodeID) -> bool:
        """Am I the single closest guard to ``key`` (elects one propagator)?"""
        gs = self._guard_set(key)
        return bool(gs) and gs[0].node_id == self.id

    async def _guard_broadcast(self, rec: GuardRecord) -> None:
        """PUT / sync a guard record to the task's ``k`` guards (point-to-point).

        Applies locally if we are a guard, and sends UPDATE_STATUS to the others.
        This is the localized state-sync that replaces global gossip.
        """
        key = task_key(rec.task_id)
        for c in self._guard_set(key):
            if c.node_id == self.id:
                self.guards.put(GuardRecord.from_dict(rec.to_dict()))
            else:
                await self.transport.send_tcp(
                    c.host, c.port,
                    Message(MessageType.UPDATE_STATUS, self.id.hex(),
                            {"record": rec.to_dict()}, ttl=0),
                )

    async def _guard_send(self, key: NodeID, mtype: MessageType, payload: dict) -> None:
        """Send a point-to-point message to all ``k`` guards of ``key``."""
        for c in self._guard_set(key):
            if c.node_id == self.id:
                await self._on_guard_msg(
                    Message(mtype, self.id.hex(), payload, ttl=0),
                    (self.host, self.port),
                )
            else:
                await self.transport.send_tcp(
                    c.host, c.port, Message(mtype, self.id.hex(), payload, ttl=0)
                )

    # ---- submitter: seed the root + children into the DHT guards -------

    async def _guard_submit_root(self) -> None:
        children_paths = expand_subtasks(self.board, [])
        if not children_paths:
            # Root cannot branch: solved outright, or immediately contradictory.
            result = solve_subtree(self.board, [], enumerate_all=True)
            if result.stats.solutions > 0 and result.board is not None:
                self.solution = result.board
                self.solutions += result.stats.solutions
                await self.gossip.broadcast(
                    Message(MessageType.SOLUTION, self.id.hex(),
                            {"board": result.board.to_flat()})
                )
            else:
                await self._declare_unsolvable()
            self._stop.set()
            return
        root = GuardRecord(task_id=path_repr([]), path=[], parent_id=None,
                           state=DONE_SPLIT,
                           children=[path_repr(p) for p in children_paths])
        await self._guard_broadcast(root)  # root record -> root's guards
        for p in children_paths:
            child = GuardRecord(task_id=path_repr(p), path=list(p),
                                parent_id=root.task_id, state=OPEN)
            await self._guard_broadcast(child)  # each child -> its guards (OPEN)
        self.log(f"[{self.id.short()}] guard-seeded root -> {len(children_paths)} "
                 f"children across the DHT")

    # ---- thief loop ---------------------------------------------------

    async def _run_guard(self) -> Board | None:
        idle = 0
        self.log(f"[{self.id.short()}] guard/thief up, waiting for work...")
        while not self._stop.is_set():
            self._maybe_tick()
            await self._guard_reclaim_expired()      # guard duty: watch leases
            await self._guard_maintain_replicas()    # guard duty: self-heal replicas
            task = self._claim_local_open()          # cold-start Opt A
            if task is None:
                task = await self._steal_random()    # random-key work stealing
            if task is None:
                idle += 1
                if self._bootstrap_contacts and idle % 30 == 0:
                    await self.dht.bootstrap(self._bootstrap_contacts)
                if idle > self.idle_limit:
                    self.log(f"[{self.id.short()}] no work after {self.idle_limit} "
                             f"polls, exiting")
                    break
                await asyncio.sleep(0.03)
                continue
            idle = 0
            await self._process_guard_task(task)
        self.log(f"[{self.id.short()}] stopping (nodes={self.nodes_expanded}, "
                 f"tasks_done={self.tasks_done}, steals={self.steals_ok}/"
                 f"{self.steals_ok + self.steals_fail})")
        return self.solution

    def _claim_local_open(self) -> Task | None:
        """Cold-start Opt A: self-claim an OPEN task we already guard locally."""
        now = time.time()
        for rec in self.guards.open_records():
            claimed = self.guards.try_claim(rec.task_id, self.id.hex(), now)
            if claimed is not None:
                asyncio.create_task(self._guard_broadcast(claimed))
                return Task(path=list(rec.path), parent_id=rec.parent_id)
        return None

    async def _steal_random(self) -> Task | None:
        """Random-key Kademlia lookup -> nearest active peer -> WORK_STEAL."""
        target = NodeID.random()
        try:
            contacts = await self.dht.lookup(target)
        except Exception:
            contacts = self.dht.table.closest(target)
        for c in contacts[:3]:
            if c.node_id == self.id:
                continue
            req = Message(MessageType.WORK_STEAL, self.id.hex(),
                          {"host": self.host, "port": self.port}, ttl=0)
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            self._pending_steal[req.msg_id] = fut
            req.payload["reply_to"] = req.msg_id
            ok = await self.transport.send_tcp(c.host, c.port, req)
            if not ok:
                self._pending_steal.pop(req.msg_id, None)
                continue
            try:
                task_dict = await asyncio.wait_for(fut, timeout=1.0)
            except asyncio.TimeoutError:
                self._pending_steal.pop(req.msg_id, None)
                continue
            if task_dict:
                self.steals_ok += 1
                return Task(path=[tuple(a) for a in task_dict["path"]],
                            parent_id=task_dict.get("parent_id"))
        self.steals_fail += 1
        return None

    async def _process_guard_task(self, task: Task) -> None:
        """Execute a stolen/claimed task: split (Opt B self-claim) or solve leaf."""
        while task is not None and not self._stop.is_set():
            self._current_task_id = task.id
            tid = task.id
            children_paths: list[Path] = []
            if task.depth < self.max_split_depth:
                children_paths = expand_subtasks(self.board, task.path)
            if len(children_paths) > 1:
                # Split T into children; PUT them to their guards.
                child_tasks = [Task(path=p, parent_id=tid) for p in children_paths]
                now = time.time()
                for i, c in enumerate(child_tasks):
                    if i == 0:
                        # Opt B: instant self-claim child[0] -> zero idle time.
                        rec = GuardRecord(task_id=c.id, path=list(c.path),
                                          parent_id=tid, state=CLAIMED,
                                          holder=self.id.hex(),
                                          lease_expire=now + self.lease_seconds)
                    else:
                        rec = GuardRecord(task_id=c.id, path=list(c.path),
                                          parent_id=tid, state=OPEN)
                    await self._guard_broadcast(rec)
                # report_split to T's guards (T -> DONE_SPLIT with children)
                await self._guard_report_split(task, [c.id for c in child_tasks])
                self.tasks_done += 1
                task = child_tasks[0]        # continue on child[0], never idle
                continue
            # Leaf: exhaustively search the subtree.
            result = solve_subtree(
                self.board, task.path,
                is_dead_end=self._is_dead_end,
                record_dead_end=self._publish_dead_end,
                should_stop=self._guard_should_stop,
                node_delay=self.node_delay,
                enumerate_all=True,
            )
            self.nodes_expanded += result.stats.nodes_expanded
            self.solutions += result.stats.solutions
            self.tasks_done += 1
            if result.stats.solutions > 0 and result.board is not None:
                # A. Solved -> gossip the solution globally to stop the swarm.
                self.solution = result.board
                self.log(f"[{self.id.short()}] FOUND solution "
                         f"(explored {self.nodes_expanded} nodes)")
                await self.gossip.broadcast(
                    Message(MessageType.SOLUTION, self.id.hex(),
                            {"board": result.board.to_flat()})
                )
                self._stop.set()
                return
            # B. Dead end -> report exhausted to this task's guards + parent tally.
            await self._guard_report_exhausted(task)
            return

    def _guard_should_stop(self) -> bool:
        self._maybe_tick()
        now = time.monotonic()
        if (self._current_task_id
                and now - self._guard_hb_last > self.lease_seconds * 0.4):
            self._guard_hb_last = now
            asyncio.create_task(self._send_heartbeat(self._current_task_id))
        return self._stop.is_set()

    async def _send_heartbeat(self, task_id: str) -> None:
        await self._guard_send(task_key(task_id), MessageType.HEARTBEAT,
                               {"task_id": task_id})

    # ---- thief -> guard reports --------------------------------------

    async def _guard_report_split(self, task: Task, children_ids: list[str]) -> None:
        await self._guard_send(
            task.key, MessageType.REPORT_SPLIT,
            {"task_id": task.id, "path": list(task.path),
             "parent_id": task.parent_id, "children": children_ids},
        )

    async def _guard_report_exhausted(self, task: Task) -> None:
        # Tell the task's own guards this branch is exhausted...
        await self._guard_send(
            task.key, MessageType.REPORT_EXHAUSTED,
            {"task_id": task.id, "path": list(task.path),
             "parent_id": task.parent_id},
        )
        # ...and tally it against the parent (bottom-up unsolvable detection).
        if task.parent_id is None:
            await self._declare_unsolvable()
        else:
            await self._guard_report_child_exhausted(task.parent_id, task.id)

    async def _guard_report_child_exhausted(self, parent_id: str, child_id: str) -> None:
        await self._guard_send(
            task_key(parent_id), MessageType.REPORT_CHILD_EXHAUSTED,
            {"parent_id": parent_id, "child_id": child_id},
        )

    # ---- guard duty: lease monitoring --------------------------------

    async def _guard_reclaim_expired(self) -> None:
        for rec in self.guards.expired_claims():
            # Only the primary guard reverts (avoids duplicate reverts / races).
            if self._is_primary_guard(task_key(rec.task_id)):
                reverted = self.guards.revert_open(rec.task_id)
                if reverted is not None:
                    self.log(f"[{self.id.short()}] lease expired on "
                             f"{rec.task_id[:16]}...; reverting to OPEN")
                    await self._guard_broadcast(reverted)

    async def _guard_maintain_replicas(self) -> None:
        """Self-heal replication: re-PUT records to the *current* guard set.

        When a guard departs, the peer now ``(k+1)``-th nearest to a key is
        promoted into that key's guard set. Periodically re-broadcasting the
        records we (as primary guard) hold transfers their state onto any
        newly-promoted guard, restoring ``k``-way replication after guard failure.
        Throttled + primary-only, so extra traffic stays O(#my-tasks)/period.
        """
        now = time.monotonic()
        if now - self._guard_maint_last < max(1.0, self.lease_seconds * 0.5):
            return
        self._guard_maint_last = now
        for rec in list(self.guards.records.values()):
            if self._is_primary_guard(task_key(rec.task_id)):
                await self._guard_broadcast(rec)

    # ---- guard inbound handlers --------------------------------------

    async def _on_guard_msg(self, msg: Message, addr) -> None:
        t = msg.type
        if t == MessageType.WORK_STEAL:
            await self._on_work_steal(msg)
        elif t == MessageType.UPDATE_STATUS:
            self.guards.apply_update(msg.payload["record"])
        elif t == MessageType.REPORT_SPLIT:
            self._on_report_split(msg)
        elif t == MessageType.REPORT_EXHAUSTED:
            self._on_report_exhausted(msg)
        elif t == MessageType.REPORT_CHILD_EXHAUSTED:
            await self._on_report_child_exhausted(msg)
        elif t == MessageType.HEARTBEAT:
            self.guards.renew(msg.payload["task_id"])
        elif t == MessageType.TASK_OFFER:
            self._on_steal_offer(msg)

    async def _on_work_steal(self, msg: Message) -> None:
        """A thief asks us (a guard) for work: hand out one OPEN task we hold."""
        thief = msg.sender
        host = msg.payload.get("host")
        port = msg.payload.get("port")
        reply_to = msg.payload.get("reply_to")
        offered = None
        for rec in self.guards.open_records():
            claimed = self.guards.try_claim(rec.task_id, thief)
            if claimed is not None:
                await self._guard_broadcast(claimed)   # sync CLAIMED to co-guards
                offered = {"path": list(rec.path), "parent_id": rec.parent_id}
                break
        if host and port:
            await self.transport.send_tcp(
                host, int(port),
                Message(MessageType.TASK_OFFER, self.id.hex(),
                        {"task": offered, "reply_to": reply_to}, ttl=0),
            )

    def _on_steal_offer(self, msg: Message) -> None:
        reply_to = msg.payload.get("reply_to")
        fut = self._pending_steal.pop(reply_to, None)
        if fut is not None and not fut.done():
            fut.set_result(msg.payload.get("task"))

    def _on_report_split(self, msg: Message) -> None:
        p = msg.payload
        tid = p["task_id"]
        if self.guards.get(tid) is None:
            self.guards.put(GuardRecord(
                task_id=tid, path=[tuple(a) for a in p.get("path", [])],
                parent_id=p.get("parent_id"), state=DONE_SPLIT,
                children=list(p.get("children", [])),
            ))
        else:
            self.guards.mark_split(tid, list(p.get("children", [])))

    def _on_report_exhausted(self, msg: Message) -> None:
        p = msg.payload
        tid = p["task_id"]
        if self.guards.get(tid) is None:
            self.guards.put(GuardRecord(
                task_id=tid, path=[tuple(a) for a in p.get("path", [])],
                parent_id=p.get("parent_id"), state=DONE_EXHAUSTED,
            ))
        else:
            self.guards.mark_exhausted(tid)

    async def _on_report_child_exhausted(self, msg: Message) -> None:
        p = msg.payload
        parent_id = p["parent_id"]
        child_id = p["child_id"]
        parent = self.guards.note_child_exhausted(parent_id, child_id)
        if parent is None:
            return  # not fully exhausted yet (or we don't hold this parent)
        # Parent just became fully exhausted; the primary guard rolls it up.
        if not self._is_primary_guard(task_key(parent_id)):
            return
        if parent.parent_id is None:
            await self._declare_unsolvable()
        else:
            await self._guard_report_child_exhausted(parent.parent_id, parent_id)

    # ---- task selection ----------------------------------------------

    def _owner_of(self, task: Task) -> Contact | None:
        """The single owner Contact for a task (exclusive mode), or None.

        ``owner_roster`` is a list of (virtual-node-id, owner-contact) pairs.
        Virtual nodes (consistent-hashing style) smooth the load imbalance that
        arises when only a few physical peers split the 160-bit XOR keyspace.
        """
        if not self.owner_roster:
            return None
        _, contact = min(self.owner_roster, key=lambda pair: xor_distance(pair[0], task.key))
        return contact

    def _owns(self, task: Task) -> bool:
        """Exclusive mode: am I the single owner (XOR-closest peer) of this task?"""
        owner = self._owner_of(task)
        if owner is not None:
            return owner.node_id == self.id
        return self.dht.is_responsible_for(task.key, replicas=1)

    async def _route_open_task(self, task: Task) -> None:
        """Publish an OPEN_TASK.

        Exclusive mode: deliver it **reliably (TCP), straight to its single
        owner** (routed by XOR key), so nothing is lost or duplicated — the DHT
        used as a put(task -> owner). ttl=0 means the owner won't re-forward it.
        Otherwise: best-effort gossip (work-stealing tolerates loss/overlap).
        """
        payload = {"task": task.to_dict()}
        if self.exclusive and self.owner_roster:
            owner = self._owner_of(task)
            if owner.node_id == self.id:
                self.scheduler.add_open(task)
            else:
                await self.transport.send_tcp(
                    owner.host, owner.port,
                    Message(MessageType.OPEN_TASK, self.id.hex(), payload, ttl=0),
                )
        else:
            self.scheduler.add_open(task)
            await self.gossip.broadcast(
                Message(MessageType.OPEN_TASK, self.id.hex(), payload)
            )

    def _pick_task(self) -> Task | None:
        """Pick the best open task: closest-to-my-ID, optionally owner-filtered."""
        reclaimed = self.scheduler.reclaim_expired()
        # Crash recovery: if a reclaimed task has a backup frontier snapshot,
        # resume from that frontier (fine-grained) instead of redoing the whole
        # subtree — only the last sync window's progress is lost.
        for t in reclaimed:
            frontier = self.scheduler.take_backup_frontier(t.id)
            if frontier:
                self.scheduler.open.pop(t.id, None)
                for p in frontier:
                    self.scheduler.add_open(Task(path=p))
        pool = list(self.scheduler.open.values())
        if self.exclusive:
            pool = [t for t in pool if self._owns(t)]
        if not pool:
            return None
        return min(pool, key=lambda t: xor_distance(self.id, t.key))

    # ---- the work loop ------------------------------------------------

    async def run(self) -> Board | None:
        """Consumer loop: pick a task -> work on it -> repeat.

        Exits when a solution is found/received (``_stop`` set) or no work is
        available for ``idle_limit`` consecutive polls — the idle window also
        gives a crashed peer's lease time to expire so its task is reclaimed.
        """
        if self.guard:
            return await self._run_guard()
        idle_rounds = 0
        self.log(f"[{self.id.short()}] waiting for tasks...")
        while not self._stop.is_set():
            self._maybe_tick()                      # refresh the live dashboard
            task = self._pick_task()                # closest-to-me, owner-filtered
            if task is None:
                idle_rounds += 1
                if idle_rounds == 1:
                    self.log(f"[{self.id.short()}] idle, waiting for tasks...")
                # Re-bootstrap every ~1s: submitter may have just started
                if self._bootstrap_contacts and idle_rounds % 30 == 0:
                    self.log(f"[{self.id.short()}] re-bootstrapping to find submitter...")
                    await self.dht.bootstrap(self._bootstrap_contacts)
                # Optimization 1: actively pull work via a random-id probe.
                if self.probe_random and idle_rounds % 10 == 0:
                    await self._probe_for_tasks()
                if idle_rounds > self.idle_limit:   # quiescent -> we're done
                    self.log(f"[{self.id.short()}] no tasks after {self.idle_limit} polls, exiting")
                    break
                await asyncio.sleep(0.03)           # wait for tasks to arrive
                continue
            idle_rounds = 0
            self.log(f"[{self.id.short()}] claiming task (depth={task.depth}, "
                     f"nodes_so_far={self.nodes_expanded})")
            await self._work_on(task)               # claim -> split/solve -> gossip
        self.log(f"[{self.id.short()}] stopping (nodes={self.nodes_expanded}, "
                 f"tasks_done={self.tasks_done})")
        return self.solution

    async def _work_on(self, task: Task) -> None:
        """Process one task end to end: claim -> split-or-solve -> publish.

        The heart of the consumer side, in order:
          1. last-moment dedup (skip if already done/dead/owned by a live peer);
          2. claim a lease and announce TASK_CLAIM;
          3. work-stealing: re-split a shallow task into finer OPEN_TASKs; else
          4. DFS the subtree, emitting DEAD_END (pruning) + SOLUTION / TASK_DONE.
        """
        # Last-moment dedup: a CLAIM/DONE for this task may have arrived after
        # we picked it. Skipping here avoids most duplicate exploration.
        tid = task.id
        if tid in self.scheduler.done or tid in self.scheduler.dead_ends:
            return
        if self.detect_unsolvable:
            await self._work_on_unsolvable(task)
            return
        if self.steal and not self.enumerate_mode:
            await self._work_on_stealing(task)
            return
        existing = self.scheduler.claimed.get(tid)
        if existing and existing.owner not in (None, self.id.hex()) and existing.lease_active():
            return

        self.scheduler.claim_local(task)
        await self.gossip.broadcast(
            Message(MessageType.TASK_CLAIM, self.id.hex(), {"task": task.to_dict()})
        )

        # Work-stealing (non-exclusive only): re-split shallow tasks so the grain
        # matches the swarm. Exclusive mode distributes statically at submit time
        # instead, which avoids the termination / late-arrival problem.
        if self.split_depth and task.depth < self.split_depth and not self.exclusive:
            children = expand_subtasks(self.board, task.path)
            if len(children) > 1:
                for child_path in children:
                    await self._route_open_task(Task(path=child_path))
                # The subtree is now covered by its children -> retire this node.
                self.scheduler.mark_done(task.path)
                await self.gossip.broadcast(
                    Message(MessageType.TASK_DONE, self.id.hex(), {"path": task.path})
                )
                return

        result = solve_subtree(
            self.board,
            task.path,
            is_dead_end=self._is_dead_end,
            record_dead_end=self._publish_dead_end,
            should_stop=self._tick_and_should_stop,
            node_delay=self.node_delay,
            enumerate_all=self.enumerate_mode,
        )
        self.nodes_expanded += result.stats.nodes_expanded
        self.solutions += result.stats.solutions
        if result.board is not None:
            self.solution = result.board

        if self.enumerate_mode:
            # Exhaustive mode: never stop early, just retire the finished task.
            self.tasks_done += 1
            self.scheduler.mark_done(task.path)
            await self.gossip.broadcast(
                Message(MessageType.TASK_DONE, self.id.hex(), {"path": task.path})
            )
            return

        if result.solved and result.board is not None:
            self.log(f"[{self.id.short()}] FOUND solution "
                     f"(explored {self.nodes_expanded} nodes)")
            await self.gossip.broadcast(
                Message(
                    MessageType.SOLUTION,
                    self.id.hex(),
                    {"board": result.board.to_flat()},
                )
            )
            self._stop.set()
        else:
            self.tasks_done += 1
            self.scheduler.mark_done(task.path)
            await self.gossip.broadcast(
                Message(MessageType.TASK_DONE, self.id.hex(), {"path": task.path})
            )

    # ---- pruning / cancel hooks ---------------------------------------

    async def _work_on_unsolvable(self, task: Task) -> None:
        """Process one task in unsolvable-detection mode.

        Either split the task one level (publishing children + a SPLIT_REPORT),
        or exhaustively search a leaf subtree and, if it holds no solution,
        report DONE_EXHAUSTED up to its parent.
        """
        tid = task.id
        if tid in self.scheduler.exhausted:
            return
        existing = self.scheduler.claimed.get(tid)
        if existing and existing.owner not in (None, self.id.hex()) and existing.lease_active():
            return

        self.scheduler.claim_local(task)
        await self.gossip.broadcast(
            Message(MessageType.TASK_CLAIM, self.id.hex(), {"task": task.to_dict()})
        )

        # Try to split one level (bounded by split_depth).
        children_paths: list[Path] = []
        if self.split_depth and task.depth < self.split_depth:
            children_paths = expand_subtasks(self.board, task.path)
        if len(children_paths) > 1:
            child_tasks = [Task(path=p, parent_id=tid) for p in children_paths]
            split_rec = Task(
                path=task.path, parent_id=task.parent_id,
                children=[t.id for t in child_tasks],
                status=TaskStatus.DONE_SPLIT,
            )
            self.scheduler.mark_split(split_rec)
            await self._replicate_split(split_rec)
            # Optimization 1c: children land in our local open pool (via
            # _route_open_task), so we continue with one next round instead of
            # re-probing with a random id.
            for t in child_tasks:
                await self._route_open_task(t)
            return

        # Leaf: exhaust the subtree (enumerate so we never stop at first path).
        result = solve_subtree(
            self.board,
            task.path,
            is_dead_end=self._is_dead_end,
            record_dead_end=self._publish_dead_end,
            should_stop=self._tick_and_should_stop,
            node_delay=self.node_delay,
            enumerate_all=True,
        )
        self.nodes_expanded += result.stats.nodes_expanded
        self.solutions += result.stats.solutions
        self.tasks_done += 1
        if result.stats.solutions > 0 and result.board is not None:
            self.solution = result.board
            self.log(f"[{self.id.short()}] FOUND solution "
                     f"(explored {self.nodes_expanded} nodes)")
            await self.gossip.broadcast(
                Message(MessageType.SOLUTION, self.id.hex(),
                        {"board": result.board.to_flat()})
            )
            self._stop.set()
            return
        # No solution in this branch -> exhausted; report up the tree.
        self.scheduler.mark_exhausted(task.path)
        await self._report_exhausted(task)

    async def _work_on_stealing(self, task: Task) -> None:
        """Explore a subtree with an explicit, *stealable* deque (Chord-style).

        Instead of a recursive DFS (whose branches are locked in the call stack
        and cannot be handed out), we keep the frontier of unexplored paths in a
        deque. We work the *tail* (LIFO -> depth-first, good locality); an idle
        peer steals from the *head* via TASK_QUERY (``_steal_from_deque``), which
        removes the branch from our deque so it is never explored twice. We yield
        to the event loop every ``steal_yield_every`` nodes so those TASK_QUERY
        requests can actually be served while we compute.
        """
        existing = self.scheduler.claimed.get(task.id)
        if existing and existing.owner not in (None, self.id.hex()) and existing.lease_active():
            return
        self.scheduler.claim_local(task)
        await self.gossip.broadcast(
            Message(MessageType.TASK_CLAIM, self.id.hex(), {"task": task.to_dict()})
        )

        self._steal_deque = deque([list(task.path)])
        self._current_task_id = task.id
        self._sync_last = time.monotonic()
        since_yield = 0
        try:
            while self._steal_deque and not self._stop.is_set():
                path = self._steal_deque.pop()          # owner end (LIFO)
                if self._is_dead_end(path):
                    continue
                try:
                    board = apply_path(self.board, path)
                except Contradiction:
                    self._publish_dead_end(path)
                    continue
                self.nodes_expanded += 1
                cell = board.most_constrained_cell()
                if cell is None:                        # complete -> solution
                    self.solution = board
                    self.log(f"[{self.id.short()}] FOUND solution "
                             f"(explored {self.nodes_expanded} nodes)")
                    await self.gossip.broadcast(
                        Message(MessageType.SOLUTION, self.id.hex(),
                                {"board": board.to_flat()})
                    )
                    self._stop.set()
                    return
                for val in board.candidates(cell):      # push children to tail
                    self._steal_deque.append(path + [(cell, val)])
                since_yield += 1
                await self._maybe_sync_state(task.id)   # periodic backup snapshot
                if self.node_delay:
                    await asyncio.sleep(self.node_delay)  # non-blocking demo cost
                    since_yield = 0
                elif since_yield >= self.steal_yield_every:
                    since_yield = 0
                    await asyncio.sleep(0)              # let TASK_QUERY be served
        finally:
            self._steal_deque = None
            self._current_task_id = None
            self.scheduler.backups.pop(task.id, None)  # our task is settled

        if not self._stop.is_set():
            # Our share drained (possibly after steals) with no solution here.
            self.tasks_done += 1
            self.scheduler.mark_done(task.path)
            await self.gossip.broadcast(
                Message(MessageType.TASK_DONE, self.id.hex(), {"path": task.path})
            )

    async def _maybe_sync_state(self, task_id: str) -> None:
        """Every ``sync_interval`` seconds, snapshot our frontier to backups."""
        if self.sync_interval <= 0 or self._steal_deque is None:
            return
        now = time.monotonic()
        if now - self._sync_last < self.sync_interval:
            return
        self._sync_last = now
        await self._sync_state(task_id, [list(p) for p in self._steal_deque])

    def _tick_and_should_stop(self) -> bool:
        # Called once per search node: cheap place to refresh the dashboard.
        self._maybe_tick()
        return self._stop.is_set()

    def _is_dead_end(self, path: Path) -> bool:
        # A node is dead if it, or any ancestor prefix, was proven invalid.
        for k in range(len(path) + 1):
            if path_repr(path[:k]) in self.scheduler.dead_ends:
                return True
        return False

    def _publish_dead_end(self, path: Path) -> None:
        # Only share shallow, high-value prunings to keep traffic bounded.
        if len(path) > self.dead_end_share_depth:
            return
        self.scheduler.mark_dead(path)
        # fire-and-forget gossip (we're inside sync DFS)
        asyncio.create_task(
            self.gossip.broadcast(
                Message(MessageType.DEAD_END, self.id.hex(), {"path": path})
            )
        )
