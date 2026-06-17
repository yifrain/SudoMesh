"""Epidemic (gossip) dissemination of OpenTasks / DeadEnds / Solution.

Why gossip (Chapter 2 + Chapter 7 intuition):
    * No central coordinator; updates reach everyone with high probability.
    * Robust to churn: a few lost messages don't break the system.
    * De-duplication via a seen-set caps redundant traffic (fan-out control).

Owner: Person C.
"""

from swarmsolve.gossip.gossip import Gossip

__all__ = ["Gossip"]
