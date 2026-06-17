"""Distributed task management: split the search tree, dedup, lease, rebalance.

A ``Task`` = a subtree of the search space, identified by an assignment Path.
Its key (``task_key``) lives in the Kademlia XOR space, so the peers closest to
that key are its natural owners -> structured placement + dedup.

Owner: Person C (splitting / placement) + Person E (leases / fault tolerance).
"""

from swarmsolve.tasks.task import Task, TaskStatus
from swarmsolve.tasks.scheduler import Scheduler

__all__ = ["Task", "TaskStatus", "Scheduler"]
