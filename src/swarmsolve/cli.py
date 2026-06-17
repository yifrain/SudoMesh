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
import multiprocessing as mp
import queue as queuemod
import time

import typer
from rich.console import Console
from rich.live import Live
from rich.table import Table

from swarmsolve.discovery.node_id import NodeID
from swarmsolve.discovery.routing import Contact
from swarmsolve.peer import Peer
from swarmsolve.puzzles import generate, load_board
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
        )
        if cfg.get("report"):
            peer.on_tick = lambda snap, r=rank: q.put(("stat", r, snap))

        boot = None
        if rank != 0:
            boot_id = NodeID.from_string(f"127.0.0.1:{BASE_PORT}")
            boot = [Contact(boot_id, "127.0.0.1", BASE_PORT)]
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
               peer.nodes_expanded, peer.tasks_done, dt, peer.solutions))

    asyncio.run(main())


def _spawn(peers: int, n: int, flat: list[int], target: int, cfg: dict):
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
            _, rank, sol, nodes, done, dt, sols = item
            results[rank] = {"sol": sol, "nodes": nodes, "done": done,
                             "dt": dt, "solutions": sols}
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
    seed: int = typer.Option(0, help="RNG seed for generation"),
) -> None:
    """Run a real local P2P swarm and compare it to the single-machine baseline."""
    board = _load_or_generate(file, size, seed)
    target = tasks or (peers * 2)
    n = board.n
    console.print(f"[bold]Puzzle {n}x{n}, peers={peers}, node_delay={node_delay}s, "
                  f"split_depth={split_depth}[/]")
    console.print(str(board))

    console.print("\n[bold]1) Single-machine baseline[/]")
    t0 = time.perf_counter()
    base = solve_local(board.clone(), node_delay=node_delay)
    base_dt = time.perf_counter() - t0
    console.print(f"   baseline time={base_dt:.3f}s  nodes={base.stats.nodes_expanded}")

    console.print("\n[bold]2) P2P swarm[/]")
    cfg = {"lease": lease, "node_delay": node_delay, "split_depth": split_depth,
           "settle": 0.6, "report": False}
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
    seed: int = typer.Option(0, help="RNG seed for generation"),
) -> None:
    """Exhaustively explore the whole tree (count all solutions / prove uniqueness).

    Unlike ``demo`` (which stops at the first solution on one deep path), this
    workload is embarrassingly parallel, so the swarm shows near-linear speedup.
    """
    board = _load_or_generate(file, size, seed)
    target = peers * 2
    n = board.n
    console.print(f"[bold]Exhaustive benchmark: {n}x{n}, peers={peers}, "
                  f"node_delay={node_delay}s[/]")

    console.print("\n[bold]1) Single-machine baseline (full tree)[/]")
    t0 = time.perf_counter()
    base = solve_local(board.clone(), node_delay=node_delay, enumerate_all=True)
    base_dt = time.perf_counter() - t0
    console.print(f"   baseline time={base_dt:.3f}s  nodes={base.stats.nodes_expanded}  "
                  f"solutions={base.stats.solutions}")

    console.print("\n[bold]2) P2P swarm (full tree)[/]")
    cfg = {"lease": lease, "node_delay": node_delay, "split_depth": split_depth,
           "settle": 0.6, "report": False, "enumerate": True}
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
) -> None:
    """Launch one real peer (use several terminals / machines for a live demo)."""
    board = load_board(file)

    async def main() -> None:
        p = Peer("0.0.0.0", port, board, log=console.print,
                 node_delay=node_delay, split_depth=split_depth, lease_seconds=lease)
        boot = None
        if bootstrap:
            host, bport = bootstrap.split(":")
            boot = [Contact(NodeID.from_string(f"{host}:{int(bport)}"), host, int(bport))]
        await p.start(boot)
        await asyncio.sleep(1.0)
        if submit:
            await p.submit(tasks)
        sol = await p.run()
        if sol:
            console.print("[green]Solution:[/]")
            console.print(str(sol))
        await p.stop()

    asyncio.run(main())


if __name__ == "__main__":
    app()
