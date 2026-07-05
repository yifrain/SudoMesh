"""Wire message definitions and JSON (de)serialization.

We use newline-delimited JSON for readability during the demo. The format is
deliberately simple so every layer can be inspected/logged easily. A binary
codec (msgpack) can be swapped in later without touching call sites.

Three *application* message types match the project brief:
    OPEN_TASK  -> an unexplored region of the search tree
    DEAD_END   -> an invalid branch that must not be re-explored
    SOLUTION   -> the final valid Sudoku solution

Plus the *infrastructure* messages used by discovery / gossip, and the
**Task Guards** RPCs (Kademlia non-exclusive mode). Under the guard model a task
is stored (PUT) on its ``k`` nearest peers, which act as *guards* tracking its
state (OPEN / CLAIMED / DONE_SPLIT / DONE_EXHAUSTED). Guards coordinate purely by
point-to-point TCP (``UPDATE_STATUS`` etc.) so state-sync traffic stays localized
to the guard group; only the final ``SOLUTION`` / unsolvable verdict is gossiped
network-wide.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class MessageType(str, Enum):
    # --- application layer (the three required types) ---
    OPEN_TASK = "OPEN_TASK"
    DEAD_END = "DEAD_END"
    SOLUTION = "SOLUTION"
    # --- task coordination ---
    TASK_CLAIM = "TASK_CLAIM"      # "I'm taking this task" (lease)
    TASK_DONE = "TASK_DONE"        # task fully explored, no solution there
    # --- task pull (random-id probing / cold start) ---
    TASK_QUERY = "TASK_QUERY"      # "do you have an open task for me?"
    TASK_OFFER = "TASK_OFFER"      # "here is an open task you may claim"
    # --- unsolvable aggregation (bottom-up) ---
    SPLIT_REPORT = "SPLIT_REPORT"          # child->parent: "I expanded into children"
    EXHAUSTED_REPORT = "EXHAUSTED_REPORT"  # child->parent: "my branch is exhausted"
    # --- periodic state sync (crash recovery for work stealing) ---
    STATE_SYNC = "STATE_SYNC"      # owner->backups: snapshot of my unexplored frontier
    # --- Task Guards (Kademlia non-exclusive mode; point-to-point TCP) ---
    # A task is PUT on its k nearest peers ("guards") who track its state. All
    # guard coordination is point-to-point TCP (no gossip) EXCEPT the final
    # SOLUTION / unsolvable verdict, which is gossiped globally.
    WORK_STEAL = "WORK_STEAL"                    # thief->guard: "give me an OPEN task"
    UPDATE_STATUS = "UPDATE_STATUS"              # guard->other k-1 guards: sync a task's state
    REPORT_SPLIT = "REPORT_SPLIT"                # thief->task guards: "I expanded it into children"
    REPORT_EXHAUSTED = "REPORT_EXHAUSTED"        # thief->task guards: "this leaf branch is invalid"
    REPORT_CHILD_EXHAUSTED = "REPORT_CHILD_EXHAUSTED"  # child guards->parent guards (recursive)
    HEARTBEAT = "HEARTBEAT"                      # thief->guard: "still alive, keep the lease"
    # --- discovery (Kademlia over UDP) ---
    PING = "PING"
    PONG = "PONG"
    FIND_NODE = "FIND_NODE"
    FIND_NODE_REPLY = "FIND_NODE_REPLY"
    # --- gossip ---
    GOSSIP_PUSH = "GOSSIP_PUSH"


@dataclass
class Message:
    """A single protocol message.

    ``sender`` is the hex NodeID of the origin. ``payload`` is type-specific.
    ``msg_id`` + ``ttl`` support gossip de-duplication and flood control.
    """

    type: MessageType
    sender: str
    payload: dict = field(default_factory=dict)
    msg_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    ttl: int = 4
    ts: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "type": self.type.value,
            "sender": self.sender,
            "payload": self.payload,
            "msg_id": self.msg_id,
            "ttl": self.ttl,
            "ts": self.ts,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Message":
        return cls(
            type=MessageType(d["type"]),
            sender=d["sender"],
            payload=d.get("payload", {}),
            msg_id=d.get("msg_id", uuid.uuid4().hex),
            ttl=d.get("ttl", 4),
            ts=d.get("ts", time.time()),
        )


def encode(msg: Message) -> bytes:
    """Serialize a message to newline-terminated UTF-8 JSON bytes."""
    return (json.dumps(msg.to_dict(), separators=(",", ":")) + "\n").encode("utf-8")


def decode(data: bytes) -> Message:
    """Deserialize bytes (one JSON object) into a Message."""
    return Message.from_dict(json.loads(data.decode("utf-8")))
