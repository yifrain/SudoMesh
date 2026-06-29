"""Wire message definitions and JSON (de)serialization.

We use newline-delimited JSON for readability during the demo. The format is
deliberately simple so every layer can be inspected/logged easily. A binary
codec (msgpack) can be swapped in later without touching call sites.

Three *application* message types match the project brief:
    OPEN_TASK  -> an unexplored region of the search tree
    DEAD_END   -> an invalid branch that must not be re-explored
    SOLUTION   -> the final valid Sudoku solution

Plus the *infrastructure* messages used by discovery / gossip.
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
    # --- work stealing (load balancing) ---
    STEAL_REQUEST = "STEAL_REQUEST"  # "Give me a task from your deque"
    STEAL_REPLY = "STEAL_REPLY"      # "Here's a task" (or empty = none available)
    # --- hybrid dead-end reporting (child -> parent, point-to-point) ---
    DEAD_END_REPORT = "DEAD_END_REPORT"  # child tells parent "this path is dead"
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
