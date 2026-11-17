from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Task:
    """A durable business unit in the task queue.

    ``Task`` deliberately carries no HTTP fields. Override
    ``Spider.start_requests(task, ctx)`` to turn a task into one or more
    :class:`pcrawl.Request` objects; the runtime schedules those requests
    independently.
    """

    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    route: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    # Reserved for queue_order="priority"; FIFO is the default queue policy.
    priority: int = 0
    retries: int = 0
    max_retries: int | None = None
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "route": self.route,
            "payload": self.payload,
            "priority": self.priority,
            "retries": self.retries,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Task":
        return cls(
            id=str(data.get("id") or uuid.uuid4().hex),
            route=str(data.get("route") or ""),
            payload=dict(data.get("payload") or {}),
            priority=int(data.get("priority") or 0),
            retries=int(data.get("retries") or 0),
            max_retries=data.get("max_retries"),
            created_at=float(data.get("created_at") or time.time()),
        )
