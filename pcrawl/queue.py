from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any

from .exceptions import ConfigError, QueueError
from .http import Request
from .settings import Settings
from .task import Task
from .utils import request_fingerprint

logger = logging.getLogger("pcrawl.queue")

_SCORE_SCALE = 1_000_000_000_000


def _validate_order(order: str) -> str:
    if order not in ("fifo", "priority"):
        raise ConfigError(f"unknown queue order: {order!r}")
    return order


def _score(priority: int, seq: int, order: str) -> int:
    if order == "fifo":
        return seq
    # zpopmin pops the lowest score: higher priority first, then earlier seq.
    return int(priority) * _SCORE_SCALE + (_SCORE_SCALE - seq % _SCORE_SCALE)


def _popped_member(popped: Any) -> Any:
    if not popped:
        return None
    if isinstance(popped, dict):
        return next(iter(popped))
    first = popped[0]
    if isinstance(first, (list, tuple)):
        return first[0]
    return first


def _matches_task(task: Task, routes: set[str] | None) -> bool:
    return routes is None or not routes or task.route in routes


def _matches_request(request: Request, routes: set[str] | None) -> bool:
    return routes is None or not routes or request.route in routes


def _dedup_accept(
    dedup: Any,
    fingerprint: str,
    ttl: float,
    *,
    now: float | None = None,
) -> bool:
    now = time.time() if now is None else now
    expires = dedup.get(fingerprint)
    if expires is not None:
        if expires == 0 or expires > now:
            return False
        del dedup[fingerprint]
    dedup[fingerprint] = 0 if ttl <= 0 else now + ttl
    return True


class TaskQueue(ABC):
    @abstractmethod
    async def open(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def put(self, task: Task) -> None:
        ...

    @abstractmethod
    async def claim(self, routes: set[str] | None = None, timeout: float = 0.0) -> Task | None:
        ...

    @abstractmethod
    async def ack(self, task: Task) -> None:
        ...

    @abstractmethod
    async def nack(self, task: Task, delay: float = 0.0) -> None:
        ...

    @abstractmethod
    async def size(self) -> int:
        ...


class RequestQueue(ABC):
    @abstractmethod
    async def open(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def put(self, request: Request, *, skip_dedup: bool = False) -> bool:
        """Queue a request; return ``False`` when it is a duplicate."""

    @abstractmethod
    async def claim(
        self,
        routes: set[str] | None = None,
        timeout: float = 0.0,
    ) -> Request | None:
        ...

    @abstractmethod
    async def ack(self, request: Request) -> None:
        ...

    @abstractmethod
    async def nack(self, request: Request, delay: float = 0.0) -> None:
        ...

    @abstractmethod
    async def size(self) -> int:
        ...


class MemoryTaskQueue(TaskQueue):
    """Thread-safe in-process queue with FIFO or priority ordering.

    It is deliberately simple: claimed tasks are removed, so a crashed worker
    loses its in-flight task. Use Redis when crash recovery matters. It is
    safe to share between threads because all mutations happen under a
    ``threading.Lock`` and each worker runs its own event loop.
    """

    def __init__(self, poll_interval: float = 0.05, order: str = "fifo") -> None:
        self._tasks: list[Task] = []
        self._lock = threading.Lock()
        self._poll_interval = poll_interval
        self._order = _validate_order(order)

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def put(self, task: Task) -> None:
        with self._lock:
            self._tasks.append(task)

    async def claim(self, routes: set[str] | None = None, timeout: float = 0.0) -> Task | None:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            with self._lock:
                if self._order == "fifo":
                    for index, task in enumerate(self._tasks):
                        if _matches_task(task, routes):
                            return self._tasks.pop(index)
                else:
                    best_index = None
                    best_key = None
                    for index, task in enumerate(self._tasks):
                        if not _matches_task(task, routes):
                            continue
                        key = (task.priority, -index)
                        if best_key is None or key > best_key:
                            best_index = index
                            best_key = key
                    if best_index is not None:
                        return self._tasks.pop(best_index)
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(self._poll_interval)

    async def ack(self, task: Task) -> None:
        return None

    async def nack(self, task: Task, delay: float = 0.0) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        await self.put(task)

    async def size(self) -> int:
        with self._lock:
            return len(self._tasks)


class SharedMemoryTaskQueue(TaskQueue):
    """A ``multiprocessing.Manager`` backed queue for local process mode.

    Claimed tasks are removed from the shared list. This makes the backend
    useful for examples and tests, but it has no visibility timeout or crash
    recovery. Redis is the production queue.
    """

    def __init__(
        self,
        tasks: Any,
        lock: Any,
        poll_interval: float = 0.05,
        order: str = "fifo",
    ) -> None:
        self._tasks = tasks
        self._lock = lock
        self._poll_interval = poll_interval
        self._order = _validate_order(order)

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def put(self, task: Task) -> None:
        with self._lock:
            self._tasks.append(task.to_dict())

    async def claim(self, routes: set[str] | None = None, timeout: float = 0.0) -> Task | None:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            with self._lock:
                if self._order == "fifo":
                    for index, raw in enumerate(self._tasks):
                        task = Task.from_dict(raw)
                        if _matches_task(task, routes):
                            self._tasks.pop(index)
                            return task
                else:
                    best_index = None
                    best_key = None
                    for index, raw in enumerate(self._tasks):
                        task = Task.from_dict(raw)
                        if not _matches_task(task, routes):
                            continue
                        key = (task.priority, -index)
                        if best_key is None or key > best_key:
                            best_index = index
                            best_key = key
                    if best_index is not None:
                        raw = self._tasks.pop(best_index)
                        return Task.from_dict(raw)
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(self._poll_interval)

    async def ack(self, task: Task) -> None:
        return None

    async def nack(self, task: Task, delay: float = 0.0) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        await self.put(task)

    async def size(self) -> int:
        with self._lock:
            return len(self._tasks)


class MemoryRequestQueue(RequestQueue):
    """In-process request queue with dedup, priority/FIFO and delayed retry."""

    def __init__(
        self,
        poll_interval: float = 0.05,
        order: str = "fifo",
        dedup_ttl: float = 0.0,
    ) -> None:
        self._requests: list[Request] = []
        self._dedup: dict[str, float] = {}
        self._lock = threading.Lock()
        self._poll_interval = poll_interval
        self._order = _validate_order(order)
        self._dedup_ttl = max(0.0, dedup_ttl)

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def put(self, request: Request, *, skip_dedup: bool = False) -> bool:
        with self._lock:
            if not skip_dedup and not request.dont_filter:
                if not _dedup_accept(
                    self._dedup,
                    request_fingerprint(request),
                    self._dedup_ttl,
                ):
                    return False
            self._requests.append(request)
            return True

    async def claim(
        self,
        routes: set[str] | None = None,
        timeout: float = 0.0,
    ) -> Request | None:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            with self._lock:
                if self._order == "fifo":
                    for index, request in enumerate(self._requests):
                        if _matches_request(request, routes):
                            return self._requests.pop(index)
                else:
                    best_index = None
                    best_key = None
                    for index, request in enumerate(self._requests):
                        if not _matches_request(request, routes):
                            continue
                        key = (request.priority, -index)
                        if best_key is None or key > best_key:
                            best_index = index
                            best_key = key
                    if best_index is not None:
                        return self._requests.pop(best_index)
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(self._poll_interval)

    async def ack(self, request: Request) -> None:
        return None

    async def nack(self, request: Request, delay: float = 0.0) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        await self.put(request, skip_dedup=True)

    async def size(self) -> int:
        with self._lock:
            return len(self._requests)


class SharedMemoryRequestQueue(RequestQueue):
    """Manager-backed request queue for local process mode."""

    def __init__(
        self,
        requests: Any,
        lock: Any,
        dedup: Any,
        poll_interval: float = 0.05,
        order: str = "fifo",
        dedup_ttl: float = 0.0,
    ) -> None:
        self._requests = requests
        self._lock = lock
        self._dedup = dedup
        self._poll_interval = poll_interval
        self._order = _validate_order(order)
        self._dedup_ttl = max(0.0, dedup_ttl)

    async def open(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def put(self, request: Request, *, skip_dedup: bool = False) -> bool:
        raw = request.to_dict()
        with self._lock:
            if not skip_dedup and not request.dont_filter:
                if not _dedup_accept(
                    self._dedup,
                    request_fingerprint(request),
                    self._dedup_ttl,
                ):
                    return False
            self._requests.append(raw)
            return True

    async def claim(
        self,
        routes: set[str] | None = None,
        timeout: float = 0.0,
    ) -> Request | None:
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            with self._lock:
                if self._order == "fifo":
                    for index, raw in enumerate(self._requests):
                        request = Request.from_dict(raw)
                        if _matches_request(request, routes):
                            self._requests.pop(index)
                            return request
                else:
                    best_index = None
                    best_key = None
                    for index, raw in enumerate(self._requests):
                        request = Request.from_dict(raw)
                        if not _matches_request(request, routes):
                            continue
                        key = (request.priority, -index)
                        if best_key is None or key > best_key:
                            best_index = index
                            best_key = key
                    if best_index is not None:
                        raw = self._requests.pop(best_index)
                        return Request.from_dict(raw)
            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(self._poll_interval)

    async def ack(self, request: Request) -> None:
        return None

    async def nack(self, request: Request, delay: float = 0.0) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        await self.put(request, skip_dedup=True)

    async def size(self) -> int:
        with self._lock:
            return len(self._requests)


class RedisTaskQueue(TaskQueue):
    """Redis zset-per-route queue with visibility timeout and delayed retry.

    Layout:

    * ``<prefix>:tasks:routes``         set of route names
    * ``<prefix>:tasks:queue:<route>``  zset task_id -> score
    * ``<prefix>:tasks:data``           hash task_id -> task JSON
    * ``<prefix>:tasks:processing``     hash task_id -> task JSON
    * ``<prefix>:tasks:deadline``       zset task_id -> visibility deadline
    * ``<prefix>:tasks:delayed``        zset task_id -> retry time
    * ``<prefix>:tasks:delayed:data``   hash task_id -> task JSON
    * ``<prefix>:tasks:seq``            insertion counter
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.prefix = settings.redis_key.rstrip(":") or "pcrawl"
        self._order = _validate_order(settings.queue_order)
        self._client: Any = None

    async def open(self) -> None:
        if self._client is not None:
            return
        try:
            from redis.asyncio import Redis
        except ImportError as exc:  # pragma: no cover - dependency is declared
            raise QueueError("redis is required for queue_backend='redis'") from exc
        self._client = Redis.from_url(self.settings.redis_url, decode_responses=True)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _key(self, suffix: str) -> str:
        return f"{self.prefix}:tasks:{suffix}"

    async def _next_score(self, priority: int) -> int:
        seq = int(await self._client.incr(self._key("seq")))
        return _score(priority, seq, self._order)

    async def put(self, task: Task) -> None:
        await self.open()
        raw = json.dumps(task.to_dict(), ensure_ascii=False)
        await self._client.hset(self._key("data"), task.id, raw)
        await self._client.zadd(
            self._key(f"queue:{task.route or ''}"),
            {task.id: await self._next_score(task.priority)},
        )
        await self._client.sadd(self._key("routes"), task.route or "")

    async def _promote_delayed(self) -> None:
        now = time.time()
        ids = await self._client.zrangebyscore(self._key("delayed"), "-inf", now)
        for task_id in ids:
            raw = await self._client.hget(self._key("delayed:data"), task_id)
            if raw is None:
                await self._client.zrem(self._key("delayed"), task_id)
                continue
            task = Task.from_dict(json.loads(raw))
            await self._client.hset(self._key("data"), task.id, raw)
            await self._client.zadd(
                self._key(f"queue:{task.route or ''}"),
                {task.id: await self._next_score(task.priority)},
            )
            await self._client.sadd(self._key("routes"), task.route or "")
            await self._client.hdel(self._key("delayed:data"), task_id)
            await self._client.zrem(self._key("delayed"), task_id)

    async def _requeue_expired(self) -> None:
        now = time.time()
        ids = await self._client.zrangebyscore(self._key("deadline"), "-inf", now)
        for task_id in ids:
            raw = await self._client.hget(self._key("processing"), task_id)
            await self._client.hdel(self._key("processing"), task_id)
            await self._client.zrem(self._key("deadline"), task_id)
            if raw is None:
                continue
            task = Task.from_dict(json.loads(raw))
            await self._client.hset(self._key("data"), task.id, raw)
            await self._client.zadd(
                self._key(f"queue:{task.route or ''}"),
                {task.id: await self._next_score(task.priority)},
            )
            await self._client.sadd(self._key("routes"), task.route or "")

    async def claim(self, routes: set[str] | None = None, timeout: float = 0.0) -> Task | None:
        await self.open()
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            await self._promote_delayed()
            await self._requeue_expired()

            if routes is None or not routes:
                route_names = list(await self._client.smembers(self._key("routes"))) or [""]
            else:
                route_names = list(routes)

            for route in route_names:
                popped = await self._client.zpopmin(self._key(f"queue:{route}"))
                if not popped:
                    continue
                task_id = _popped_member(popped)
                raw = await self._client.hget(self._key("data"), task_id)
                if raw is None:
                    continue
                task = Task.from_dict(json.loads(raw))
                await self._client.hset(self._key("processing"), task.id, raw)
                await self._client.zadd(
                    self._key("deadline"),
                    {task.id: time.time() + self.settings.visibility_timeout},
                )
                return task

            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(self.settings.claim_poll_interval)

    async def ack(self, task: Task) -> None:
        await self.open()
        await self._client.hdel(self._key("processing"), task.id)
        await self._client.hdel(self._key("data"), task.id)
        await self._client.zrem(self._key("deadline"), task.id)

    async def nack(self, task: Task, delay: float = 0.0) -> None:
        await self.open()
        await self._client.hdel(self._key("processing"), task.id)
        await self._client.zrem(self._key("deadline"), task.id)
        raw = await self._client.hget(self._key("data"), task.id)
        if raw is None:
            raw = json.dumps(task.to_dict(), ensure_ascii=False)
        await self._client.hdel(self._key("data"), task.id)
        if delay > 0:
            await self._client.hset(self._key("delayed:data"), task.id, raw)
            await self._client.zadd(
                self._key("delayed"),
                {task.id: time.time() + delay},
            )
            return
        await self._client.hset(self._key("data"), task.id, raw)
        await self._client.zadd(
            self._key(f"queue:{task.route or ''}"),
            {task.id: await self._next_score(task.priority)},
        )
        await self._client.sadd(self._key("routes"), task.route or "")

    async def size(self) -> int:
        await self.open()
        route_names = await self._client.smembers(self._key("routes"))
        total = 0
        for route in route_names:
            total += int(await self._client.zcard(self._key(f"queue:{route}")))
        total += int(await self._client.hlen(self._key("processing")))
        total += int(await self._client.zcard(self._key("delayed")))
        return total


class RedisRequestQueue(RequestQueue):
    """Redis request queue with dedup, priority/FIFO and delayed retry.

    Layout:

    * ``<prefix>:requests:routes``         set
    * ``<prefix>:requests:queue:<route>``  zset request_id -> score
    * ``<prefix>:requests:data``           hash request_id -> request JSON
    * ``<prefix>:requests:processing``     hash request_id -> request JSON
    * ``<prefix>:requests:deadline``       zset request_id -> visibility deadline
    * ``<prefix>:requests:delayed``        zset request_id -> retry time
    * ``<prefix>:requests:delayed:data``   hash request_id -> request JSON
    * ``<prefix>:requests:dedup``          zset fingerprint -> expiry
    * ``<prefix>:requests:seq``            insertion counter
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.prefix = settings.redis_key.rstrip(":") or "pcrawl"
        self._order = _validate_order(settings.queue_order)
        self._dedup_ttl = max(0.0, settings.request_dedup_ttl)
        self._client: Any = None

    async def open(self) -> None:
        if self._client is not None:
            return
        try:
            from redis.asyncio import Redis
        except ImportError as exc:  # pragma: no cover - dependency is declared
            raise QueueError("redis is required for queue_backend='redis'") from exc
        self._client = Redis.from_url(self.settings.redis_url, decode_responses=True)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _key(self, suffix: str) -> str:
        return f"{self.prefix}:requests:{suffix}"

    async def _next_score(self, priority: int) -> int:
        seq = int(await self._client.incr(self._key("seq")))
        return _score(priority, seq, self._order)

    async def _dedup_accept(self, request: Request) -> bool:
        fingerprint = request_fingerprint(request)
        key = self._key("dedup")
        now = time.time()
        score = await self._client.zscore(key, fingerprint)
        if score is not None:
            score = float(score)
            if score == 0 or score > now:
                return False
            await self._client.zrem(key, fingerprint)
        await self._client.zadd(
            key,
            {fingerprint: 0 if self._dedup_ttl <= 0 else now + self._dedup_ttl},
        )
        return True

    async def put(self, request: Request, *, skip_dedup: bool = False) -> bool:
        await self.open()
        if not skip_dedup and not request.dont_filter:
            if not await self._dedup_accept(request):
                return False
        raw = json.dumps(request.to_dict(), ensure_ascii=False)
        await self._client.hset(self._key("data"), request.id, raw)
        await self._client.zadd(
            self._key(f"queue:{request.route or ''}"),
            {request.id: await self._next_score(request.priority)},
        )
        await self._client.sadd(self._key("routes"), request.route or "")
        return True

    async def _promote_delayed(self) -> None:
        now = time.time()
        ids = await self._client.zrangebyscore(self._key("delayed"), "-inf", now)
        for request_id in ids:
            raw = await self._client.hget(self._key("delayed:data"), request_id)
            if raw is None:
                await self._client.zrem(self._key("delayed"), request_id)
                continue
            request = Request.from_dict(json.loads(raw))
            await self._client.hset(self._key("data"), request.id, raw)
            await self._client.zadd(
                self._key(f"queue:{request.route or ''}"),
                {request.id: await self._next_score(request.priority)},
            )
            await self._client.sadd(self._key("routes"), request.route or "")
            await self._client.hdel(self._key("delayed:data"), request_id)
            await self._client.zrem(self._key("delayed"), request_id)

    async def _requeue_expired(self) -> None:
        now = time.time()
        ids = await self._client.zrangebyscore(self._key("deadline"), "-inf", now)
        for request_id in ids:
            raw = await self._client.hget(self._key("processing"), request_id)
            await self._client.hdel(self._key("processing"), request_id)
            await self._client.zrem(self._key("deadline"), request_id)
            if raw is None:
                continue
            request = Request.from_dict(json.loads(raw))
            await self._client.hset(self._key("data"), request.id, raw)
            await self._client.zadd(
                self._key(f"queue:{request.route or ''}"),
                {request.id: await self._next_score(request.priority)},
            )
            await self._client.sadd(self._key("routes"), request.route or "")

    async def claim(
        self,
        routes: set[str] | None = None,
        timeout: float = 0.0,
    ) -> Request | None:
        await self.open()
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            await self._promote_delayed()
            await self._requeue_expired()

            if routes is None or not routes:
                route_names = list(await self._client.smembers(self._key("routes"))) or [""]
            else:
                route_names = list(routes)

            for route in route_names:
                popped = await self._client.zpopmin(self._key(f"queue:{route}"))
                if not popped:
                    continue
                request_id = _popped_member(popped)
                raw = await self._client.hget(self._key("data"), request_id)
                if raw is None:
                    continue
                request = Request.from_dict(json.loads(raw))
                await self._client.hset(self._key("processing"), request.id, raw)
                await self._client.zadd(
                    self._key("deadline"),
                    {request.id: time.time() + self.settings.visibility_timeout},
                )
                return request

            if time.monotonic() >= deadline:
                return None
            await asyncio.sleep(self.settings.claim_poll_interval)

    async def ack(self, request: Request) -> None:
        await self.open()
        await self._client.hdel(self._key("processing"), request.id)
        await self._client.hdel(self._key("data"), request.id)
        await self._client.zrem(self._key("deadline"), request.id)

    async def nack(self, request: Request, delay: float = 0.0) -> None:
        await self.open()
        await self._client.hdel(self._key("processing"), request.id)
        await self._client.zrem(self._key("deadline"), request.id)
        raw = await self._client.hget(self._key("data"), request.id)
        if raw is None:
            raw = json.dumps(request.to_dict(), ensure_ascii=False)
        await self._client.hdel(self._key("data"), request.id)
        if delay > 0:
            await self._client.hset(self._key("delayed:data"), request.id, raw)
            await self._client.zadd(
                self._key("delayed"),
                {request.id: time.time() + delay},
            )
            return
        await self._client.hset(self._key("data"), request.id, raw)
        await self._client.zadd(
            self._key(f"queue:{request.route or ''}"),
            {request.id: await self._next_score(request.priority)},
        )
        await self._client.sadd(self._key("routes"), request.route or "")

    async def size(self) -> int:
        await self.open()
        route_names = await self._client.smembers(self._key("routes"))
        total = 0
        for route in route_names:
            total += int(await self._client.zcard(self._key(f"queue:{route}")))
        total += int(await self._client.hlen(self._key("processing")))
        total += int(await self._client.zcard(self._key("delayed")))
        return total


def make_task_queue(settings: Settings, shared: TaskQueue | None = None) -> TaskQueue:
    if settings.queue_backend == "redis":
        return RedisTaskQueue(settings)
    if shared is not None:
        return shared
    return MemoryTaskQueue(
        poll_interval=min(0.05, settings.claim_poll_interval),
        order=settings.queue_order,
    )


def make_request_queue(
    settings: Settings,
    shared: RequestQueue | None = None,
) -> RequestQueue:
    if settings.queue_backend == "redis":
        return RedisRequestQueue(settings)
    if shared is not None:
        return shared
    return MemoryRequestQueue(
        poll_interval=min(0.05, settings.claim_poll_interval),
        order=settings.queue_order,
        dedup_ttl=settings.request_dedup_ttl,
    )


def make_shared_task_queue(tasks: Any, lock: Any, order: str = "fifo") -> SharedMemoryTaskQueue:
    return SharedMemoryTaskQueue(tasks, lock, order=order)


def make_shared_request_queue(
    requests: Any,
    lock: Any,
    dedup: Any,
    order: str = "fifo",
    dedup_ttl: float = 0.0,
) -> SharedMemoryRequestQueue:
    return SharedMemoryRequestQueue(
        requests,
        lock,
        dedup,
        order=order,
        dedup_ttl=dedup_ttl,
    )


def shared_task_queue_config(task_queue: SharedMemoryTaskQueue) -> dict[str, Any]:
    return {
        "kind": "shared",
        "tasks": task_queue._tasks,
        "lock": task_queue._lock,
        "order": task_queue._order,
    }


def shared_request_queue_config(queue: SharedMemoryRequestQueue) -> dict[str, Any]:
    return {
        "kind": "shared",
        "requests": queue._requests,
        "lock": queue._lock,
        "dedup": queue._dedup,
        "order": queue._order,
        "dedup_ttl": queue._dedup_ttl,
    }


def task_queue_config(settings: Settings) -> dict[str, Any]:
    if settings.queue_backend == "redis":
        return redis_task_queue_config(settings)
    raise ConfigError("process queue config requires a shared or redis queue")


def redis_task_queue_config(settings: Settings) -> dict[str, Any]:
    return {
        "kind": "redis",
        "redis_url": settings.redis_url,
        "redis_key": settings.redis_key,
        "visibility_timeout": settings.visibility_timeout,
        "claim_poll_interval": settings.claim_poll_interval,
        "queue_order": settings.queue_order,
    }


def request_queue_config(settings: Settings) -> dict[str, Any]:
    if settings.queue_backend == "redis":
        return redis_request_queue_config(settings)
    raise ConfigError("process request queue config requires a shared or redis queue")


def redis_request_queue_config(settings: Settings) -> dict[str, Any]:
    return {
        "kind": "redis",
        "redis_url": settings.redis_url,
        "redis_key": settings.redis_key,
        "visibility_timeout": settings.visibility_timeout,
        "claim_poll_interval": settings.claim_poll_interval,
        "queue_order": settings.queue_order,
        "request_dedup_ttl": settings.request_dedup_ttl,
    }


def _redis_settings(config: dict[str, Any]) -> Settings:
    return Settings(
        queue_backend="redis",
        redis_url=config["redis_url"],
        redis_key=config["redis_key"],
        visibility_timeout=config["visibility_timeout"],
        claim_poll_interval=config["claim_poll_interval"],
        queue_order=config.get("queue_order", "fifo"),
        request_dedup_ttl=config.get("request_dedup_ttl", 0.0),
    )


def task_queue_from_config(config: dict[str, Any]) -> TaskQueue:
    kind = config.get("kind")
    if kind == "shared":
        return SharedMemoryTaskQueue(
            config["tasks"],
            config["lock"],
            order=config.get("order", "fifo"),
        )
    if kind == "redis":
        return RedisTaskQueue(_redis_settings(config))
    raise ConfigError(f"unknown process queue config: {kind!r}")


def request_queue_from_config(config: dict[str, Any]) -> RequestQueue:
    kind = config.get("kind")
    if kind == "shared":
        return SharedMemoryRequestQueue(
            config["requests"],
            config["lock"],
            config["dedup"],
            order=config.get("order", "fifo"),
            dedup_ttl=config.get("dedup_ttl", 0.0),
        )
    if kind == "redis":
        return RedisRequestQueue(_redis_settings(config))
    raise ConfigError(f"unknown process request queue config: {kind!r}")


def ensure_tasks(results: Iterable[Any]) -> list[Task]:
    tasks: list[Task] = []
    for result in results:
        if isinstance(result, Task):
            tasks.append(result)
        else:
            raise TypeError(f"start_tasks must yield Task objects, got {type(result)!r}")
    return tasks
