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

from swarmsolve.discovery.kademlia import KademliaNode
from swarmsolve.discovery.node_id import NodeID, xor_distance
from swarmsolve.discovery.routing import Contact
from swarmsolve.gossip.gossip import Gossip
from swarmsolve.solver.board import Board
from swarmsolve.solver.search import (
    Path,
    expand_subtasks,
    solve_subtree,
)
from swarmsolve.tasks.scheduler import Scheduler
from swarmsolve.tasks.task import Task, path_repr
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
            }
        )
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
        elif msg.type == MessageType.SOLUTION:
            # termination link: the first solution stops the whole swarm
            if self.solution is None:
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
        tasks = self.seed_frontier(target_tasks)
        self.log(f"[{self.id.short()}] seeding {len(tasks)} open tasks")
        for t in tasks:
            await self._route_open_task(t)

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
        self.scheduler.reclaim_expired()
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
