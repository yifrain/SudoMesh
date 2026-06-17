"""SwarmSolve - a decentralized P2P system for solving very large Sudoku puzzles.

Layered architecture (bottom-up):
    transport  -> message passing & serialization (TCP/UDP)
    discovery  -> decentralized peer discovery (Kademlia DHT, XOR keyspace)
    gossip     -> epidemic propagation of OpenTasks / DeadEnds / Solution
    tasks      -> search-tree splitting, dedup, load balancing, leases
    solver     -> constraint propagation + DFS Sudoku engine

See README.md for the full design.
"""

__version__ = "0.1.0"
