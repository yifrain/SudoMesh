"""Command-line interface for SwarmSolve.

Commands
--------
    swarmsolve gen        -> generate a puzzle file (any size N=k*k)
    swarmsolve solve      -> single-machine solve (baseline)
    swarmsolve demo       -> REAL local swarm (N processes) vs baseline, with
                             work-stealing + an optional per-node cost so the
                             parallel speedup is real and measurable        [B]
    swarmsolve dashboard  -> same swarm, with a live rich dashboard          [C]
    swarmsolve fault      -> kill a peer mid-solve; its lease expires and the
                             task is reassigned; the swarm still finishes    [A]
    swarmsolve peer       -> launch one long-running peer (manual / multi-machine)

``demo``/``dashboard``/``fault`` spawn separate OS processes that talk over real
localhost sockets, so the CPU-bound search runs in parallel for real.
"""

from __future__ import annotations

import asyncio
import csv
import multiprocessing as mp
import queue as queuemod
import statistics
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.live import Live
from rich.table import Table

from swarmsolve.discovery.node_id import NodeID
from swarmsolve.discovery.routing import Contact
from swarmsolve.peer import Peer
from swarmsolve.puzzles import generate, load_board, make_unsolvable
from swarmsolve.solver.board import Board
from swarmsolve.solver.search import solve_local

app = typer.Typer(add_completion=False, help="SwarmSolve - P2P Sudoku solver")
console = Console()

BASE_PORT = 9000


# --------------------------------------------------------------------------- #
# shared multi-process worker
# --------------------------------------------------------------------------- #
def _peer_worker(
    rank: int,
    n: int,
    puzzle_flat: list[int],
    target_tasks: int,
    cfg: dict,
    q: "mp.Queue",
) -> None:
    """Run one peer in its own process. Reports stats/results back via ``q``."""
    board = Board.from_flat(n, puzzle_flat)

    async def main() -> None:
        peer = Peer(
            "127.0.0.1",
            BASE_PORT + rank,
            board,
            log=lambda *a: None,
            lease_seconds=cfg["lease"],
            node_delay=cfg["node_delay"],
            split_depth=cfg["split_depth"],
            enumerate_mode=cfg.get("enumerate", False),
            idle_limit=cfg.get("idle_limit", 30),
            exclusive=cfg.get("exclusive", False),
            owner_roster=(
                [(NodeID.from_string(f"127.0.0.1:{BASE_PORT + r}#vn{v}"),
                  Contact(NodeID.from_string(f"127.0.0.1:{BASE_PORT + r}"),
                          "127.0.0.1", BASE_PORT + r))
                 for r in range(cfg["peers"])
                 for v in range(cfg.get("vnodes", 16))]
                if cfg.get("exclusive") else None
            ),
            probe_random=cfg.get("probe_random", False),
            detect_unsolvable=cfg.get("detect_unsolvable", False),
            root_replicas=cfg.get("root_replicas", 3),
            steal=cfg.get("steal", False),
            sync_interval=cfg.get("sync_interval", 0.0),
            guard=cfg.get("guard", False),
            guard_k=cfg.get("guard_k", 3),
            max_split_depth=cfg.get("max_split_depth", cfg.get("split_depth", 2)),
        )
        if cfg.get("report"):
            peer.on_tick = lambda snap, r=rank: q.put(("stat", r, snap))

        boot = None
        if rank != 0:
            boot_id = NodeID.from_string(f"127.0.0.1:{BASE_PORT}")
            boot = [Contact(boot_id, "127.0.0.1", BASE_PORT)]
        # Churn experiment: late-joining peers start after ``join_delay`` so we
        # can measure dynamic-join self-scalability (do they still get work?).
        if rank >= cfg.get("join_rank", cfg["peers"]):
            await asyncio.sleep(cfg.get("join_delay", 0.0))
        await peer.start(boot)
        await asyncio.sleep(cfg["settle"])

        t0 = time.perf_counter()
        if rank == 0:
            await peer.submit(target_tasks)
        sol = await peer.run()
        dt = time.perf_counter() - t0

        if cfg.get("report"):
            q.put(("stat", rank, peer.snapshot()))
        await peer.stop()
        q.put(("result", rank, sol.to_flat() if sol else None,
               peer.nodes_expanded, peer.tasks_done, dt, peer.solutions,
               peer.unsolvable))

    asyncio.run(main())


def _spawn(peers: int, n: int, flat: list[int], target: int, cfg: dict):
    cfg = {**cfg, "peers": peers}  # workers need the roster size for ownership
    ctx = mp.get_context("spawn")
    q: mp.Queue = ctx.Queue()
    procs = [
        ctx.Process(target=_peer_worker, args=(r, n, flat, target, cfg, q))
        for r in range(peers)
    ]
    for p in procs:
        p.start()
    return procs, q


def _collect(procs, q, on_stat=None) -> dict:
    """Drain the queue until every process has exited. Robust to killed peers."""
    results: dict = {}
    while any(p.is_alive() for p in procs) or not q.empty():
        try:
            item = q.get(timeout=0.1)
        except queuemod.Empty:
            continue
        if item[0] == "result":
            _, rank, sol, nodes, done, dt, sols = item[:7]
            uns = item[7] if len(item) > 7 else False
            results[rank] = {"sol": sol, "nodes": nodes, "done": done,
                             "dt": dt, "solutions": sols, "unsolvable": uns}
        elif item[0] == "stat" and on_stat is not None:
            on_stat(item[1], item[2])
    return results


def _load_or_generate(file: str, size: int, seed: int) -> Board:
    return load_board(file) if file else generate(size, seed=seed)


# --------------------------------------------------------------------------- #
# gen
# --------------------------------------------------------------------------- #
@app.command()
def gen(
    size: int = typer.Option(16, help="Board size N (must be a perfect square)"),
    out: str = typer.Option("puzzle.txt", help="Output file"),
    clue_ratio: float = typer.Option(0.35, help="Fraction of cells kept as clues"),
    seed: int = typer.Option(0, help="RNG seed"),
) -> None:
    """Generate a solvable puzzle and write it to a file."""
    board = generate(size, clue_ratio=clue_ratio, seed=seed)
    grid = board.to_grid()
    width = len(str(size))
    lines = [" ".join(str(v).rjust(width) if v else "." * width for v in row) for row in grid]
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    console.print(f"[green]Wrote {size}x{size} puzzle to {out}[/]")
    console.print(str(board))


# --------------------------------------------------------------------------- #
# solve (baseline)
# --------------------------------------------------------------------------- #
@app.command()
def solve(
    file: str = typer.Argument(..., help="Puzzle file"),
    node_delay: float = typer.Option(0.0, help="Artificial per-node cost (demo)"),
) -> None:
    """Solve a puzzle on a single machine (baseline)."""
    board = load_board(file)
    console.print(f"[bold]Solving {board.n}x{board.n} locally...[/]")
    t0 = time.perf_counter()
    result = solve_local(board, node_delay=node_delay)
    dt = time.perf_counter() - t0
    if result.solved and result.board:
        console.print("[green]Solved![/]")
        console.print(str(result.board))
    else:
        console.print("[red]No solution found.[/]")
    console.print(
        f"time={dt:.3f}s  nodes={result.stats.nodes_expanded}  "
        f"dead_ends={result.stats.dead_ends}"
    )


# --------------------------------------------------------------------------- #
# demo (real multi-process swarm) — [B] work-stealing + real speedup
# --------------------------------------------------------------------------- #
@app.command()
def demo(
    file: str = typer.Option("", help="Puzzle file (else one is generated)"),
    size: int = typer.Option(16, help="Generated puzzle size if no file given"),
    peers: int = typer.Option(4, help="Number of peers (processes)"),
    tasks: int = typer.Option(0, help="Target #subtasks to seed (0 = 2*peers)"),
    node_delay: float = typer.Option(
        0.003, help="Per-node cost (s); models expensive search so speedup shows"
    ),
    split_depth: int = typer.Option(2, help="Work-stealing: re-split tasks below this depth"),
    lease: float = typer.Option(5.0, help="Task lease seconds"),
    steal: bool = typer.Option(
        False, help="Fine-grained work stealing: idle peers steal branches from "
                    "busy peers' deque (Chord-style) instead of only gossip re-split"
    ),
    sync_interval: float = typer.Option(
        0.0, help="Periodic state sync (s): busy peers snapshot their frontier to "
                  "backups for crash recovery (0 = off; needs --steal)"
    ),
    guard: bool = typer.Option(
        False, help="Task-Guards mode (Kademlia non-exclusive): tasks are stored on "
                    "their k nearest peers (guards); idle peers random-key-lookup + "
                    "WORK_STEAL; all state-sync is point-to-point TCP, gossip only for "
                    "the final solution"
    ),
    guard_k: int = typer.Option(3, help="Replication factor k (guards per task)"),
    seed: int = typer.Option(0, help="RNG seed for generation"),
) -> None:
    """Run a real local P2P swarm and compare it to the single-machine baseline."""
    board = _load_or_generate(file, size, seed)
    target = tasks or (peers * 2)
    n = board.n
    mode = ("task-guards" if guard
            else "work-stealing deque" if steal else "gossip + re-split")
    console.print(f"[bold]Puzzle {n}x{n}, peers={peers}, node_delay={node_delay}s, "
                  f"split_depth={split_depth}, mode={mode}[/]")
    console.print(str(board))

    console.print("\n[bold]1) Single-machine baseline[/]")
    t0 = time.perf_counter()
    base = solve_local(board.clone(), node_delay=node_delay)
    base_dt = time.perf_counter() - t0
    console.print(f"   baseline time={base_dt:.3f}s  nodes={base.stats.nodes_expanded}")

    console.print("\n[bold]2) P2P swarm[/]")
    cfg = {"lease": lease, "node_delay": node_delay, "split_depth": split_depth,
           "settle": 0.6, "report": False, "steal": steal,
           "probe_random": steal, "sync_interval": sync_interval,
           "guard": guard, "guard_k": guard_k, "max_split_depth": split_depth,
           "idle_limit": 60 if (steal or guard) else 30}
    procs, q = _spawn(peers, n, board.to_flat(), target, cfg)
    swarm_t0 = time.perf_counter()
    results = _collect(procs, q)
    for p in procs:
        p.join()
    swarm_dt = time.perf_counter() - swarm_t0

    _print_results_table(results, n, peers)

    console.print("\n[bold]3) Summary[/]")
    total_nodes = sum(r["nodes"] for r in results.values())
    console.print(f"   baseline : {base_dt:.3f}s, {base.stats.nodes_expanded} nodes")
    console.print(f"   swarm    : {swarm_dt:.3f}s wall, {total_nodes} nodes across {peers} peers")
    if swarm_dt > 0:
        sp = base_dt / swarm_dt
        color = "green" if sp >= 1 else "yellow"
        console.print(f"   [{color}]speedup  : {sp:.2f}x (wall clock)[/]")


def _print_results_table(results: dict, n: int, peers: int) -> None:
    table = Table(title="Per-peer report")
    for col in ("peer", "found", "nodes", "tasks_done", "time(s)"):
        table.add_column(col)
    solution_flat = None
    for rank in range(peers):
        r = results.get(rank)
        if r is None:
            table.add_row(str(rank), "[red]KILLED[/]", "-", "-", "-")
            continue
        if r["sol"]:
            solution_flat = r["sol"]
        table.add_row(str(rank), "YES" if r["sol"] else "-",
                      str(r["nodes"]), str(r["done"]), f"{r['dt']:.3f}")
    console.print(table)
    if solution_flat:
        console.print("[green]Swarm solved![/]")
        console.print(str(Board.from_flat(n, solution_flat)))
    else:
        console.print("[red]Swarm did not find a solution.[/]")


# --------------------------------------------------------------------------- #
# benchmark — [B] exhaustive search: near-linear, honest speedup
# --------------------------------------------------------------------------- #
@app.command()
def benchmark(
    file: str = typer.Option("", help="Puzzle file (else one is generated)"),
    size: int = typer.Option(16, help="Generated puzzle size if no file given"),
    peers: int = typer.Option(4, help="Number of peers (processes)"),
    node_delay: float = typer.Option(
        0.0006, help="Per-node cost (s); models expensive search"
    ),
    split_depth: int = typer.Option(2, help="Work-stealing depth"),
    lease: float = typer.Option(8.0, help="Task lease seconds"),
    exclusive: bool = typer.Option(
        True, help="Deterministic single-owner execution (no duplicate work)"
    ),
    vnodes: int = typer.Option(16, help="Virtual nodes per peer (load smoothing)"),
    seed: int = typer.Option(0, help="RNG seed for generation"),
) -> None:
    """Exhaustively explore the whole tree (count all solutions / prove uniqueness).

    Unlike ``demo`` (which stops at the first solution on one deep path), this
    workload is embarrassingly parallel, so the swarm shows near-linear speedup.
    """
    board = _load_or_generate(file, size, seed)
    # Exclusive distributes statically at submit time → seed a finer frontier for
    # balance; work-stealing refines at runtime from a coarse seed.
    target = peers * 64 if exclusive else peers * 2
    n = board.n
    mode = "exclusive (single-owner)" if exclusive else "work-stealing (may duplicate)"
    console.print(f"[bold]Exhaustive benchmark: {n}x{n}, peers={peers}, "
                  f"node_delay={node_delay}s, mode={mode}[/]")

    console.print("\n[bold]1) Single-machine baseline (full tree)[/]")
    t0 = time.perf_counter()
    base = solve_local(board.clone(), node_delay=node_delay, enumerate_all=True)
    base_dt = time.perf_counter() - t0
    console.print(f"   baseline time={base_dt:.3f}s  nodes={base.stats.nodes_expanded}  "
                  f"solutions={base.stats.solutions}")

    console.print("\n[bold]2) P2P swarm (full tree)[/]")
    cfg = {"lease": lease, "node_delay": node_delay, "split_depth": split_depth,
           "settle": 0.5, "report": False, "enumerate": True, "vnodes": vnodes,
           "exclusive": exclusive, "idle_limit": 30 if exclusive else 60}
    procs, q = _spawn(peers, n, board.to_flat(), target, cfg)
    swarm_t0 = time.perf_counter()
    results = _collect(procs, q)
    for p in procs:
        p.join()
    swarm_dt = time.perf_counter() - swarm_t0

    total_nodes = sum(r["nodes"] for r in results.values())
    total_solutions = sum(r["solutions"] for r in results.values())

    table = Table(title="Per-peer report (exhaustive)")
    for col in ("peer", "nodes", "tasks_done", "solutions", "time(s)"):
        table.add_column(col)
    for rank in range(peers):
        r = results.get(rank)
        if r is None:
            table.add_row(str(rank), "-", "-", "-", "-")
        else:
            table.add_row(str(rank), str(r["nodes"]), str(r["done"]),
                          str(r["solutions"]), f"{r['dt']:.3f}")
    console.print(table)

    console.print("\n[bold]3) Summary[/]")
    console.print(f"   baseline : {base_dt:.3f}s, {base.stats.nodes_expanded} nodes, "
                  f"{base.stats.solutions} solutions")
    console.print(f"   swarm    : {swarm_dt:.3f}s wall, {total_nodes} nodes, "
                  f"{total_solutions} solutions across {peers} peers")
    if total_solutions == base.stats.solutions:
        console.print(f"   [green]correctness OK: all {base.stats.solutions} "
                      f"solution(s) covered exactly once[/]")
    elif total_solutions > base.stats.solutions:
        console.print(f"   [yellow]all {base.stats.solutions} solution(s) covered; "
                      f"{total_solutions - base.stats.solutions} duplicate hit(s) from "
                      f"concurrent exploration (the 'avoid duplicate work' challenge)[/]")
    else:
        console.print(f"   [red]WARNING: only {total_solutions}/{base.stats.solutions} "
                      f"solution(s) found[/]")
    if swarm_dt > 0:
        sp = base_dt / swarm_dt
        color = "green" if sp >= 1 else "yellow"
        console.print(f"   [{color}]speedup  : {sp:.2f}x (wall clock)[/]")


# --------------------------------------------------------------------------- #
# unsolvable — prove NO solution via bottom-up DONE_EXHAUSTED aggregation
# --------------------------------------------------------------------------- #
@app.command()
def unsolvable(
    file: str = typer.Option("", help="Unsolvable puzzle file (else generated)"),
    size: int = typer.Option(9, help="Generated puzzle size if no file given"),
    peers: int = typer.Option(4, help="Number of peers (processes)"),
    node_delay: float = typer.Option(0.0, help="Per-node cost (s); demo knob"),
    split_depth: int = typer.Option(2, help="Tree split depth (more/smaller subtasks)"),
    lease: float = typer.Option(8.0, help="Task lease seconds"),
    guard: bool = typer.Option(
        False, help="Use Task-Guards mode: exhaustion is aggregated bottom-up via "
                    "guard REPORT_CHILD_EXHAUSTED RPCs (point-to-point) instead of gossip"
    ),
    guard_k: int = typer.Option(3, help="Replication factor k (guards per task)"),
    seed: int = typer.Option(21, help="RNG seed for the generated unsolvable puzzle"),
) -> None:
    """Prove a puzzle has NO solution via hierarchical unsolvable detection.

    Peers split the tree, exhaust leaf branches, and report DONE_EXHAUSTED up to
    their parents; when the (replicated) root task becomes exhausted, the swarm
    declares the puzzle unsolvable. In ``--guard`` mode this roll-up runs over the
    Task-Guards point-to-point RPCs; otherwise it uses gossip.
    """
    board = load_board(file) if file else make_unsolvable(size, seed=seed, clue_ratio=0.30)
    n = board.n
    console.print(f"[bold]Unsolvable detection: {n}x{n}, peers={peers}, "
                  f"split_depth={split_depth}[/]")
    console.print(str(board))

    console.print("\n[bold]1) Single-machine baseline[/]")
    t0 = time.perf_counter()
    base = solve_local(board.clone(), node_delay=node_delay)
    base_dt = time.perf_counter() - t0
    verdict = "SOLVED" if base.solved else "NO SOLUTION"
    console.print(f"   baseline: {verdict} in {base_dt:.3f}s, "
                  f"{base.stats.nodes_expanded} nodes")

    console.print("\n[bold]2) P2P swarm (bottom-up DONE_EXHAUSTED aggregation)[/]")
    cfg = {"lease": lease, "node_delay": node_delay, "split_depth": split_depth,
           "settle": 0.5, "report": False,
           "detect_unsolvable": not guard, "probe_random": not guard,
           "guard": guard, "guard_k": guard_k, "max_split_depth": split_depth,
           "root_replicas": min(peers, 3), "idle_limit": 60}
    procs, q = _spawn(peers, n, board.to_flat(), peers * 4, cfg)
    swarm_t0 = time.perf_counter()
    results = _collect(procs, q)
    for p in procs:
        p.join()
    swarm_dt = time.perf_counter() - swarm_t0

    any_unsolvable = any(r.get("unsolvable") for r in results.values())
    any_solution = any(r.get("sol") for r in results.values())
    total_nodes = sum(r["nodes"] for r in results.values())

    table = Table(title="Per-peer report (unsolvable detection)")
    for col in ("peer", "nodes", "tasks_done", "verdict"):
        table.add_column(col)
    for rank in range(peers):
        r = results.get(rank)
        if r is None:
            table.add_row(str(rank), "-", "-", "-")
        else:
            v = ("[red]UNSOLVABLE[/]" if r.get("unsolvable")
                 else "[green]SOLVED[/]" if r.get("sol") else "-")
            table.add_row(str(rank), str(r["nodes"]), str(r["done"]), v)
    console.print(table)

    console.print("\n[bold]3) Summary[/]")
    console.print(f"   baseline : {verdict}")
    console.print(f"   swarm    : {swarm_dt:.3f}s wall, {total_nodes} nodes "
                  f"across {peers} peers")
    if any_solution:
        console.print("   [green]swarm found a SOLUTION[/]")
    elif any_unsolvable:
        console.print("   [red]swarm proved the puzzle UNSOLVABLE "
                      "(root task exhausted, bottom-up)[/]")
    else:
        console.print("   [yellow]swarm did not reach a verdict "
                      "(try more peers / higher split_depth / idle_limit)[/]")
    if base.solved == any_solution and (not base.solved) == any_unsolvable:
        console.print("   [green]verdict matches single-machine baseline[/]")


# --------------------------------------------------------------------------- #
# dashboard — [C] live visualization
# --------------------------------------------------------------------------- #
def _dashboard_table(stats: dict, peers: int) -> Table:
    table = Table(title="SwarmSolve — live dashboard")
    for col in ("peer", "id", "neighbors", "open", "claimed", "dead", "done",
                "nodes", "found"):
        table.add_column(col)
    for rank in range(peers):
        s = stats.get(rank)
        if s is None:
            table.add_row(str(rank), "…", "-", "-", "-", "-", "-", "-", "-")
            continue
        found = "[green]YES[/]" if s.get("found") else "-"
        table.add_row(
            str(rank), s.get("id", "?"), str(s.get("peers", 0)),
            str(s.get("open", 0)), str(s.get("claimed", 0)),
            str(s.get("dead_ends", 0)), str(s.get("done", 0)),
            str(s.get("nodes", 0)), found,
        )
    return table


@app.command()
def dashboard(
    file: str = typer.Option("", help="Puzzle file (else one is generated)"),
    size: int = typer.Option(16, help="Generated puzzle size if no file given"),
    peers: int = typer.Option(4, help="Number of peers (processes)"),
    tasks: int = typer.Option(0, help="Target #subtasks to seed (0 = 2*peers)"),
    node_delay: float = typer.Option(0.01, help="Per-node cost (s) for a visible pace"),
    split_depth: int = typer.Option(2, help="Work-stealing depth"),
    lease: float = typer.Option(6.0, help="Task lease seconds"),
    seed: int = typer.Option(0, help="RNG seed for generation"),
) -> None:
    """Run the swarm with a live dashboard of per-peer task counters."""
    board = _load_or_generate(file, size, seed)
    target = tasks or (peers * 2)
    n = board.n
    console.print(f"[bold]Live swarm: {n}x{n}, peers={peers}[/]")

    cfg = {"lease": lease, "node_delay": node_delay, "split_depth": split_depth,
           "settle": 0.6, "report": True}
    procs, q = _spawn(peers, n, board.to_flat(), target, cfg)

    stats: dict = {}
    with Live(_dashboard_table(stats, peers), console=console, refresh_per_second=8) as live:
        def on_stat(rank, snap):
            stats[rank] = snap
            live.update(_dashboard_table(stats, peers))
        results = _collect(procs, q, on_stat=on_stat)
        live.update(_dashboard_table(stats, peers))
    for p in procs:
        p.join()

    console.print()
    _print_results_table(results, n, peers)


# --------------------------------------------------------------------------- #
# fault — [A] fault tolerance demo
# --------------------------------------------------------------------------- #
@app.command()
def fault(
    file: str = typer.Option("", help="Puzzle file (else one is generated)"),
    size: int = typer.Option(16, help="Generated puzzle size if no file given"),
    peers: int = typer.Option(4, help="Number of peers (processes)"),
    kill_peer: int = typer.Option(2, help="Which peer to kill mid-solve"),
    kill_after: float = typer.Option(1.6, help="Seconds before killing it"),
    lease: float = typer.Option(1.5, help="Task lease seconds (short -> fast recovery)"),
    node_delay: float = typer.Option(0.02, help="Per-node cost (s) to widen the window"),
    split_depth: int = typer.Option(3, help="Work-stealing depth (more, finer tasks)"),
    seed: int = typer.Option(0, help="RNG seed for generation"),
) -> None:
    """Kill a peer mid-solve and show its leased task gets reassigned."""
    board = _load_or_generate(file, size, seed)
    target = peers * 2
    n = board.n
    console.print(f"[bold]Fault-tolerance demo: {n}x{n}, peers={peers}[/]")
    console.print(f"plan: kill peer #{kill_peer} after {kill_after}s "
                  f"(lease={lease}s -> its task should be reclaimed)\n")

    # Exhaustive mode + generous idle_limit: every task MUST be completed, so a
    # crashed peer's task must be reclaimed for the run to finish -> this proves
    # the reassignment actually happened.
    cfg = {"lease": lease, "node_delay": node_delay, "split_depth": split_depth,
           "settle": 0.6, "report": False, "enumerate": True, "idle_limit": 200}
    procs, q = _spawn(peers, n, board.to_flat(), target, cfg)

    swarm_t0 = time.perf_counter()
    time.sleep(kill_after)
    if 0 <= kill_peer < peers and procs[kill_peer].is_alive():
        procs[kill_peer].terminate()
        console.print(f"[red]>>> killed peer #{kill_peer} (PID {procs[kill_peer].pid})[/]")

    results = _collect(procs, q)
    for p in procs:
        p.join()
    swarm_dt = time.perf_counter() - swarm_t0

    console.print()
    _print_results_table(results, n, peers)
    solved = any(r["sol"] for r in results.values())
    survivors = [r for r in results if r != kill_peer]
    console.print("\n[bold]Result[/]")
    console.print(f"   killed peer #{kill_peer} returned a result: "
                  f"{'no (as expected)' if kill_peer not in results else 'yes'}")
    console.print(f"   surviving peers that finished: {sorted(survivors)}")
    if solved:
        console.print(f"   [green]swarm STILL solved the puzzle in {swarm_dt:.2f}s "
                      f"despite the failure[/]")
    else:
        console.print("   [red]swarm failed to solve (try larger node_delay / lease)[/]")


# --------------------------------------------------------------------------- #
# peer (single long-running node, for manual demos)
# --------------------------------------------------------------------------- #
@app.command()
def peer(
    port: int = typer.Option(9000, help="This peer's port"),
    file: str = typer.Option(..., help="Puzzle file (all peers must share it)"),
    bootstrap: str = typer.Option("", help="host:port of a known peer (empty = first)"),
    submit: bool = typer.Option(False, help="Seed the task frontier from this peer"),
    tasks: int = typer.Option(16, help="Target #subtasks to seed when --submit"),
    node_delay: float = typer.Option(0.0, help="Artificial per-node cost (demo)"),
    split_depth: int = typer.Option(0, help="Work-stealing depth (0 = off)"),
    lease: float = typer.Option(10.0, help="Task lease seconds"),
    idle_limit: int = typer.Option(30, help="Idle polls before giving up (higher = wait longer for tasks)"),
) -> None:
    """Launch one real peer (use several terminals / machines for a live demo)."""
    board = load_board(file)

    async def main() -> None:
        p = Peer("0.0.0.0", port, board, log=console.print,
                 node_delay=node_delay, split_depth=split_depth, lease_seconds=lease,
                 idle_limit=idle_limit)
        boot = None
        if bootstrap:
            host, bport = bootstrap.split(":")
            boot = [Contact(NodeID.from_string(f"{host}:{int(bport)}"), host, int(bport))]
        await p.start(boot)
        if submit:
            await asyncio.sleep(2.0)     # let joiners bootstrap
            await p.submit(tasks)
            await asyncio.sleep(1.0)     # let gossip propagate to joiners
        else:
            await asyncio.sleep(1.0)
        sol = await p.run()
        if sol:
            console.print("[green]Solution:[/]")
            console.print(str(sol))
        await p.stop()

    asyncio.run(main())


# --------------------------------------------------------------------------- #
# evaluate — quantitative Evaluation experiments (for the project report)
# --------------------------------------------------------------------------- #
def _run_swarm(n, flat, peers, target, cfg, *, kill_peer=None, kill_after=0.0):
    """Spawn a swarm, optionally kill one peer mid-run, return (wall_dt, results)."""
    procs, q = _spawn(peers, n, flat, target, cfg)
    t0 = time.perf_counter()
    if kill_peer is not None:
        time.sleep(kill_after)
        if 0 <= kill_peer < peers and procs[kill_peer].is_alive():
            procs[kill_peer].terminate()
    results = _collect(procs, q)
    for p in procs:
        p.join()
    return time.perf_counter() - t0, results


def _write_csv(out: "Path | None", name: str, header: list, rows: list) -> None:
    if out is None:
        return
    path = out / name
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    console.print(f"   [dim]wrote {path}[/]")


def _eval_scaling(board, n, peers_list, repeats, node_delay, split_depth, out):
    """Experiment 1: self-scalability — speedup/efficiency/throughput vs #peers."""
    console.print("\n[bold]Experiment 1 — Self-scalability[/] "
                  "(exhaustive search, exclusive single-owner mode)")
    base_times = []
    base = None
    for _ in range(repeats):
        t0 = time.perf_counter()
        base = solve_local(board.clone(), node_delay=node_delay, enumerate_all=True)
        base_times.append(time.perf_counter() - t0)
    base_dt = statistics.median(base_times)
    console.print(f"   baseline (1 machine): median {base_dt:.3f}s, "
                  f"{base.stats.solutions} solutions, {base.stats.nodes_expanded} nodes")

    plist = [int(x) for x in peers_list.split(",") if x.strip()]
    rows = []
    one_dt = None
    for p in plist:
        cfg = {"lease": 8.0, "node_delay": node_delay, "split_depth": split_depth,
               "settle": 0.5, "report": False, "enumerate": True, "exclusive": True,
               "vnodes": 16, "idle_limit": 30}
        dts, nodes, sols = [], 0, 0
        for _ in range(repeats):
            dt, res = _run_swarm(n, board.to_flat(), p, p * 64, cfg)
            dts.append(dt)
            nodes = sum(r["nodes"] for r in res.values())
            sols = sum(r["solutions"] for r in res.values())
        med = statistics.median(dts)
        if p == 1:
            one_dt = med
        rows.append([p, round(med, 3), round(base_dt / med, 2),
                     round(one_dt / med, 2) if one_dt else "-",
                     round((one_dt / med) / p, 2) if one_dt else "-",
                     int(nodes / med) if med else 0, sols])

    table = Table(title="Self-scalability")
    for c in ("peers", "wall(s)", "speedup/base", "speedup/1", "efficiency",
              "throughput(nodes/s)", "solutions"):
        table.add_column(c)
    for r in rows:
        table.add_row(*[str(x) for x in r])
    console.print(table)
    _write_csv(out, "scaling.csv",
               ["peers", "wall_s", "speedup_vs_base", "speedup_vs_1",
                "efficiency", "throughput_nodes_per_s", "solutions"], rows)


def _eval_resilience(board, n, peers, repeats, node_delay, split_depth, out):
    """Experiment 2: resilience — completion + recovery overhead under a crash."""
    console.print("\n[bold]Experiment 2 — Resilience[/] "
                  "(random crash, exhaustive so every task MUST complete)")
    cfg = {"lease": 1.5, "node_delay": node_delay,
           "split_depth": max(split_depth, 3), "settle": 0.5, "report": False,
           "enumerate": True, "idle_limit": 80}

    no_fault = []
    # Ground-truth exact solution count (single machine, no delay -> fast).
    exact = solve_local(board.clone(), enumerate_all=True).stats.solutions
    for _ in range(repeats):
        dt, res = _run_swarm(n, board.to_flat(), peers, peers * 2, cfg)
        sols = sum(r["solutions"] for r in res.values())
        no_fault.append((dt, sols >= exact))
    base_med = statistics.median([d for d, _ in no_fault])
    base_ok = 100.0 * sum(1 for _, ok in no_fault if ok) / len(no_fault)
    kill_after = max(0.6, base_med * 0.4)

    killed = []
    for _ in range(repeats):
        dt, res = _run_swarm(n, board.to_flat(), peers, peers * 2, cfg,
                             kill_peer=peers // 2, kill_after=kill_after)
        sols = sum(r["solutions"] for r in res.values())
        # All solutions still found (>= exact) => the crashed peer's task was
        # reclaimed and re-explored; nothing was lost. (Non-exclusive mode may
        # over-count via duplication, so we test >= exact, not ==.)
        killed.append((dt, sols >= exact))
    kill_med = statistics.median([d for d, _ in killed])
    success = 100.0 * sum(1 for _, ok in killed if ok) / len(killed)

    rows = [
        ["no-fault", repeats, round(base_ok, 1), round(base_med, 3), "0.000"],
        [f"kill-1 (peer {peers // 2})", repeats, round(success, 1),
         round(kill_med, 3), round(kill_med - base_med, 3)],
    ]
    table = Table(title=f"Resilience (peers={peers}, kill@{kill_after:.1f}s, "
                        f"exact solutions={exact})")
    for c in ("scenario", "runs", "completion %", "median wall(s)", "recovery overhead(s)"):
        table.add_column(c)
    for r in rows:
        table.add_row(*[str(x) for x in r])
    console.print(table)
    console.print("   [dim]completion % = runs that still found ALL solutions "
                  "(>= exact count) despite the crash — i.e. the dead peer's task "
                  "was reclaimed via lease expiry and re-explored.[/]")
    _write_csv(out, "resilience.csv",
               ["scenario", "runs", "completion_pct", "median_wall_s",
                "recovery_overhead_s"], rows)


def _eval_churn(board, n, peers, node_delay, split_depth, out):
    """Experiment 3: churn — do late-joining peers pick up work (dynamic scaling)?"""
    console.print("\n[bold]Experiment 3 — Churn / dynamic join[/] "
                  "(half the peers join late; work stealing enabled)")
    join_rank = max(1, peers // 2)
    cfg = {"lease": 8.0, "node_delay": node_delay,
           "split_depth": max(split_depth, 3), "settle": 0.5, "report": False,
           "enumerate": True, "exclusive": False, "idle_limit": 100,
           "steal": True, "probe_random": True,
           "join_rank": join_rank, "join_delay": 1.5}
    dt, res = _run_swarm(n, board.to_flat(), peers, peers * 2, cfg)

    total = sum(r["nodes"] for r in res.values()) or 1
    late = sum(r["nodes"] for rank, r in res.items() if rank >= join_rank)
    rows = []
    for rank in range(peers):
        r = res.get(rank)
        joined = "late" if rank >= join_rank else "early"
        if r is None:
            rows.append([rank, joined, 0, 0])
        else:
            rows.append([rank, joined, r["nodes"], r["done"]])
    table = Table(title=f"Churn (peers={peers}, late joiners join +1.5s)")
    for c in ("peer", "join", "nodes", "tasks_done"):
        table.add_column(c)
    for r in rows:
        table.add_row(*[str(x) for x in r])
    console.print(table)
    console.print(f"   wall={dt:.3f}s · late joiners did "
                  f"[bold]{100.0 * late / total:.1f}%[/] of the work "
                  f"→ the swarm absorbed nodes that joined after tasks were seeded "
                  f"(self-scalability under churn).")
    _write_csv(out, "churn.csv", ["peer", "join", "nodes", "tasks_done"], rows)


@app.command()
def evaluate(
    suite: str = typer.Option("all", help="scaling | resilience | churn | all"),
    file: str = typer.Option("", help="Puzzle file (else one is generated)"),
    size: int = typer.Option(9, help="Generated puzzle size if no file given"),
    seed: int = typer.Option(7, help="RNG seed for generation"),
    peers_list: str = typer.Option("1,2,4", help="Peer counts for the scaling sweep"),
    peers: int = typer.Option(4, help="Peers for resilience/churn experiments"),
    repeats: int = typer.Option(3, help="Repetitions per data point (median)"),
    node_delay: float = typer.Option(0.0003, help="Per-node cost (s); models expensive search"),
    split_depth: int = typer.Option(3, help="Split / work-stealing depth"),
    clue_ratio: float = typer.Option(
        0.28, help="Clue fraction when generating (lower = bigger search tree). "
                   "A wide tree is needed for a meaningful exhaustive workload."
    ),
    csv_dir: str = typer.Option("", help="If set, write CSV files here for plotting"),
) -> None:
    """Quantitative Evaluation experiments for the project report.

    Produces the numbers/figures the report's Evaluation chapter needs:
    self-scalability (speedup/efficiency/throughput vs #peers), resilience
    (completion + recovery overhead under a random crash), and churn (late
    joiners picking up work). Use ``--csv-dir`` to dump CSVs for plotting.

    NOTE: meaningful parallel speedup needs a substantial workload, so by default
    we generate a *wide* puzzle (low clue ratio -> large exhaustive search tree).
    A tiny puzzle is dominated by process-startup / settle overhead.
    """
    board = load_board(file) if file else generate(size, clue_ratio=clue_ratio, seed=seed)
    n = board.n
    out = Path(csv_dir) if csv_dir else None
    if out is not None:
        out.mkdir(parents=True, exist_ok=True)
    console.print(f"[bold]Evaluation on {n}x{n} (seed={seed}, clue_ratio={clue_ratio}), "
                  f"repeats={repeats}[/]")

    if suite in ("scaling", "all"):
        _eval_scaling(board, n, peers_list, repeats, node_delay, split_depth, out)
    if suite in ("resilience", "all"):
        _eval_resilience(board, n, peers, repeats, node_delay, split_depth, out)
    if suite in ("churn", "all"):
        _eval_churn(board, n, peers, node_delay, split_depth, out)


if __name__ == "__main__":
    app()
