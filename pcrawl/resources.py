from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from typing import Any

from .exceptions import ConfigError, ResourceInvalid, ResourceUnavailable
from .outcomes import ResourceOutcome
from .settings import PurserMode, Settings

logger = logging.getLogger("pcrawl.resources")


def resource_types_from_adapters(adapters: Any) -> dict[str, type]:
    """Map adapter kind -> resource class for typed IPC deserialization."""

    types: dict[str, type] = {}
    for adapter in adapters or ():
        kind = getattr(adapter, "kind", None)
        cls = getattr(adapter, "resource_class", None)
        if kind and isinstance(cls, type):
            types[str(kind)] = cls
    return types


def resource_type_paths(adapters: Any) -> dict[str, str]:
    """Importable ``module:qualname`` paths for handing types to workers."""

    paths: dict[str, str] = {}
    for kind, cls in resource_types_from_adapters(adapters).items():
        module = getattr(cls, "__module__", "")
        qualname = getattr(cls, "__qualname__", "")
        if module and qualname and "<locals>" not in qualname:
            paths[kind] = f"{module}:{qualname}"
    return paths


def lease_request_payload(value: Any) -> dict[str, Any]:
    """Normalize a purser ``AcquireRequest`` or mapping into a JSON-safe dict."""

    if value is None:
        return {}
    if is_dataclass(value) and not isinstance(value, type):
        data = {item.name: getattr(value, item.name) for item in fields(value)}
    elif isinstance(value, Mapping):
        data = dict(value)
    else:
        raise TypeError(
            "resource lease must be a purser AcquireRequest, mapping or None; "
            f"got {type(value)!r}"
        )
    if isinstance(data.get("tags"), (set, frozenset)):
        data["tags"] = sorted(data["tags"])
    return data


def _coerce_outcome(value: Any) -> ResourceOutcome:
    if isinstance(value, ResourceOutcome):
        return value
    raw = getattr(value, "value", value)
    try:
        return ResourceOutcome(str(raw))
    except ValueError as exc:
        raise ValueError(f"unknown resource outcome: {value!r}") from exc


@dataclass(frozen=True)
class ImportReport:
    """Backend-neutral view of one ``import_resources`` call."""

    imported: int = 0
    updated: int = 0
    skipped: int = 0
    errors: tuple[Mapping[str, Any], ...] = ()
    validation: tuple[Mapping[str, Any], ...] = ()
    batch_id: str | None = None

    @property
    def total(self) -> int:
        return self.imported + self.updated + self.skipped + len(self.errors)

    @classmethod
    def from_value(cls, value: Any) -> "ImportReport":
        if isinstance(value, cls):
            return value
        if is_dataclass(value) and not isinstance(value, type):
            data = {item.name: getattr(value, item.name) for item in fields(value)}
        elif isinstance(value, Mapping):
            data = dict(value)
        else:
            return cls()
        return cls(
            imported=int(data.get("imported") or 0),
            updated=int(data.get("updated") or 0),
            skipped=int(data.get("skipped") or 0),
            errors=tuple(data.get("errors") or ()),
            validation=tuple(data.get("validation") or ()),
            batch_id=data.get("batch_id"),
        )


class PoolHandle:
    """A purser-shaped pool handle exposing only provisioning operations."""

    def __init__(self, runtime: "ResourceRuntime", name: str, kind: str) -> None:
        self.runtime = runtime
        self.name = name
        self.kind = kind

    async def import_resources(self, records: Any, **kwargs: Any) -> ImportReport:
        return await self.runtime.import_resources(self.name, records, **kwargs)

    async def stats(self) -> Any:
        return await self.runtime.stats(self.name)

    def __repr__(self) -> str:
        return f"PoolHandle(name={self.name!r}, kind={self.kind!r})"


class ResourceRuntime:
    """Purser-shaped resource facade handed to ``Spider.start_resources``.

    The facade owns the concrete backend selected from the execution mode:
    embedded ``Purser`` for inline/async, parent-owned ``LocalScheduler`` for
    thread/process. It is created lazily on the first adapter/pool declaration,
    so spiders without resources never import purser.
    """

    def __init__(self, settings: Settings, mode: str) -> None:
        self.settings = settings
        self.mode = mode
        self._adapters: list[Any] = []
        self._pools: dict[str, PoolHandle] = {}
        self._purser: Any = None
        self._purser_pools: dict[str, Any] = {}
        self._scheduler: Any = None
        self._backend: ResourceBackend | None = None
        self._resolved: PurserMode | None = None
        self._started = False
        self._closed = False

        if settings.purser_mode == "embedded" and mode in ("thread", "process"):
            raise ConfigError(
                "embedded purser cannot be shared across threads/processes; "
                "use purser_mode='local' or leave it on 'auto'"
            )

    def register_adapter(self, adapter: Any) -> None:
        if self._pools:
            raise ConfigError("register adapters before creating pools")
        self._adapters.append(adapter)

    def create_pool(
        self,
        name: str,
        kind: str,
        *,
        config: Mapping[str, Any] | None = None,
        profile: Any = None,
        **overrides: Any,
    ) -> PoolHandle:
        name = str(name)
        if name in self._pools:
            raise ConfigError(f"pool {name!r} is already declared")
        self._ensure_backend(declaring=True)
        options = dict(overrides)
        options.setdefault("lease_ttl", self.settings.lease_ttl)

        if self._purser is not None:
            pool = self._purser.create_pool(
                name,
                kind,
                config=config,
                profile=profile,
                **options,
            )
            self._purser_pools[name] = pool
        else:
            self._scheduler.create_pool(
                name,
                kind,
                config=config,
                profile=profile,
                **options,
            )

        handle = PoolHandle(self, name, str(kind))
        self._pools[name] = handle
        return handle

    async def import_resources(
        self,
        pool: str,
        records: Any,
        **kwargs: Any,
    ) -> ImportReport:
        if pool not in self._pools:
            raise ConfigError(
                f"unknown pool {pool!r}; declared pools: {sorted(self._pools)}"
            )
        await self.start()
        if self._purser is not None:
            report = await self._purser_pools[pool].import_resources(
                records,
                **kwargs,
            )
        else:
            report = await asyncio.to_thread(
                self._scheduler.import_resources,
                pool,
                records,
                **kwargs,
            )
        return ImportReport.from_value(report)

    async def stats(self, pool: str) -> Any:
        if pool not in self._pools:
            raise ConfigError(
                f"unknown pool {pool!r}; declared pools: {sorted(self._pools)}"
            )
        await self.start()
        if self._purser is not None:
            return await self._purser_pools[pool].stats()
        return await asyncio.to_thread(self._scheduler.stats, pool)

    async def start(self) -> None:
        if self._resolved is None:
            self._resolved = self._resolve_mode()
        if self._purser is not None and not self._started:
            await self._purser.start()
        self.settings.purser_mode = self._resolved
        self._started = True

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._purser is not None:
            await self._purser.close()
        if self._scheduler is not None:
            await asyncio.to_thread(self._scheduler.close)
        self._purser = None
        self._scheduler = None

    @property
    def resolved_mode(self) -> PurserMode:
        return self._resolved or self._resolve_mode()

    @property
    def adapters(self) -> tuple[Any, ...]:
        return tuple(self._adapters)

    @property
    def pool_names(self) -> tuple[str, ...]:
        return tuple(self._pools)

    @property
    def scheduler_address(self) -> str | None:
        if self._scheduler is None:
            return None
        return self._scheduler.address.to_env()

    @property
    def backend(self) -> ResourceBackend:
        if self._backend is None:
            self._backend = self._make_backend()
        return self._backend

    def _resolve_mode(self, *, declaring: bool = False) -> PurserMode:
        if not self._pools and not declaring:
            return "off"
        configured = self.settings.purser_mode
        if configured != "auto":
            return configured
        return "embedded" if self.mode in ("inline", "async") else "local"

    def _ensure_backend(self, *, declaring: bool = False) -> None:
        if self._resolved is not None:
            if declaring and self._resolved == "off":
                raise ConfigError(
                    "resource pools were declared but purser_mode is 'off'"
                )
            return
        mode = self._resolve_mode(declaring=declaring)
        if mode == "off":
            raise ConfigError(
                "resource pools were declared but purser_mode is 'off'"
            )
        if mode == "embedded":
            from purser import Purser

            self._purser = Purser(
                storage=self.settings.purser_storage,
                coordination=self.settings.purser_coordination,
                sqlite_path=self.settings.purser_sqlite_path,
                redis_url=self.settings.purser_redis_url,
            )
            for adapter in self._adapters:
                self._purser.register_adapter(adapter)
        elif mode == "local":
            from purser.local import LocalScheduler

            self._scheduler = LocalScheduler(
                storage=self.settings.purser_storage,
                coordination=self.settings.purser_coordination,
                sqlite_path=self.settings.purser_sqlite_path,
                redis_url=self.settings.purser_redis_url,
                start_method=self.settings.purser_start_method,
                adapters=tuple(self._adapters),
            )
            self._scheduler.start()
        self._resolved = mode

    def _make_backend(self) -> ResourceBackend:
        if self._purser is not None:
            return _EmbeddedBackend(
                self.settings,
                purser=self._purser,
                pools=self._purser_pools,
            )
        if self._scheduler is not None:
            return _LocalBackend(
                self._scheduler.address,
                self.settings,
                resource_types=resource_types_from_adapters(self._adapters),
            )
        return _NoopBackend()


class LeaseHandle:
    """A thin, backend-neutral view of one purser lease."""

    def __init__(
        self,
        *,
        name: str,
        pool: str,
        raw: Any,
        backend: "ResourceBackend",
        use_limit: int | None = None,
    ) -> None:
        self.name = name
        self.pool = pool
        self.raw = raw
        self._backend = backend
        self.use_limit = use_limit
        self._used = 0
        self.outcome = ResourceOutcome.OK
        self.released = False

    @property
    def token(self) -> str:
        return str(getattr(self.raw, "token", ""))

    @property
    def resource_id(self) -> str:
        return str(getattr(self.raw, "resource_id", ""))

    @property
    def payload(self) -> dict[str, Any]:
        value = getattr(self.raw, "payload", None)
        return dict(value) if isinstance(value, Mapping) else {}

    @property
    def resource(self) -> Any:
        return getattr(self.raw, "resource", None)

    @property
    def tags(self) -> frozenset[str]:
        resource = self.resource
        tags = getattr(resource, "tags", None) if resource is not None else None
        if tags:
            return frozenset(tags)
        return frozenset()

    @property
    def used(self) -> int:
        return self._used

    def mark_used(self, count: int = 1) -> None:
        if self.released:
            raise RuntimeError("lease has already been released")
        if count < 0:
            raise ValueError("count must be >= 0")
        self._used += count
        raw_used = getattr(self.raw, "used", None)
        if raw_used is not None:
            try:
                self.raw.used = self._used
            except Exception:
                logger.debug("could not update raw lease used counter", exc_info=True)
        if self.use_limit is not None and self._used > self.use_limit:
            raise ResourceInvalid(ResourceOutcome.QUOTA_EXHAUSTED)

    def set_outcome(self, outcome: Any) -> None:
        self.outcome = _coerce_outcome(outcome)

    async def renew(self) -> bool:
        if self.released:
            return False
        return await self._backend.renew(self)

    async def release(
        self,
        outcome: Any = None,
        used: int | None = None,
    ) -> bool:
        if self.released:
            return True
        if outcome is not None:
            self.outcome = _coerce_outcome(outcome)
        self.released = True
        return await self._backend.release(
            self,
            outcome=self.outcome,
            used=self._used if used is None else used,
        )


class WorkerSession:
    """One worker-session epoch: leases plus user-owned state.

    A session owns its session-scoped leases, renews them while the worker is
    active, and releases them together when the worker session ends.
    """

    def __init__(
        self,
        *,
        manager: "ResourceManager",
        worker_id: str,
        heartbeat_interval: float | None = None,
    ) -> None:
        self.id = uuid.uuid4().hex
        self.manager = manager
        self.worker_id = worker_id
        self.leases: dict[str, LeaseHandle] = {}
        self.user: dict[str, Any] = {}
        self.outcome = ResourceOutcome.OK
        self.heartbeat_error: BaseException | None = None
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_task: asyncio.Task[None] | None = None

    def get(self, name: str) -> LeaseHandle:
        try:
            return self.leases[name]
        except KeyError as exc:
            available = ", ".join(self.leases) or "<none>"
            raise KeyError(f"resource {name!r} is not leased; available: {available}") from exc

    def __getitem__(self, name: str) -> LeaseHandle:
        return self.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self.leases

    async def acquire(
        self,
        pool: str,
        request: Any = None,
        **overrides: Any,
    ) -> LeaseHandle:
        if pool in self.leases:
            return self.leases[pool]
        try:
            handle = await self.manager.acquire(
                pool,
                request,
                consumer=f"{self.worker_id}:{self.id}",
                **overrides,
            )
        except (ResourceInvalid, ResourceUnavailable):
            raise
        except Exception as exc:
            raise ResourceUnavailable(
                f"could not acquire session resource {pool!r}: {exc}"
            ) from exc
        self.leases[pool] = handle
        if self._heartbeat_interval is not None:
            self.start_heartbeat(self._heartbeat_interval)
        return handle

    async def release(
        self,
        name: str,
        outcome: Any = None,
        used: int | None = None,
    ) -> bool:
        handle = self.leases.pop(name, None)
        if handle is None:
            return True
        return await handle.release(outcome=outcome, used=used)

    def invalidate(self, outcome: Any) -> None:
        final = _coerce_outcome(outcome)
        self.outcome = final
        for handle in self.leases.values():
            handle.set_outcome(final)

    def start_heartbeat(self, interval: float) -> None:
        if self._heartbeat_task is not None or not self.leases:
            return
        self._heartbeat_task = asyncio.create_task(self._heartbeat(max(0.01, interval)))

    async def stop_heartbeat(self) -> None:
        task, self._heartbeat_task = self._heartbeat_task, None
        if task is None or task.done():
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return

    async def _heartbeat(self, interval: float) -> None:
        while True:
            await asyncio.sleep(interval)
            for handle in list(self.leases.values()):
                if handle.released:
                    continue
                try:
                    await handle.renew()
                except Exception as exc:
                    self.heartbeat_error = exc
                    logger.warning(
                        "lease heartbeat failed for %s/%s: %s",
                        self.worker_id,
                        handle.name,
                        exc,
                    )

    async def release_all(self, outcome: Any = None) -> None:
        await self.stop_heartbeat()
        final_outcome = self.outcome if outcome is None else _coerce_outcome(outcome)
        for name in reversed(list(self.leases)):
            try:
                await self.release(name, outcome=final_outcome)
            except Exception:
                logger.exception("failed to release resource %s", name)
        self.leases.clear()


class ResourceBackend:
    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def acquire(
        self,
        pool: str,
        *,
        request: Mapping[str, Any],
    ) -> LeaseHandle:
        raise NotImplementedError

    async def release(
        self,
        handle: LeaseHandle,
        *,
        outcome: ResourceOutcome,
        used: int,
    ) -> bool:
        raise NotImplementedError

    async def renew(self, handle: LeaseHandle) -> bool:
        return True

    async def import_resources(self, pool: str, records: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def stats(self, pool: str) -> Any:
        raise NotImplementedError


class _NoopBackend(ResourceBackend):
    async def acquire(
        self,
        pool: str,
        *,
        request: Mapping[str, Any],
    ) -> LeaseHandle:
        raise ResourceUnavailable(
            "resource backend is off; call resources.create_pool(...) in "
            "start_resources or set purser_mode='embedded'/'local'"
        )

    async def release(self, handle: LeaseHandle, *, outcome: ResourceOutcome, used: int) -> bool:
        return True

    async def import_resources(self, pool: str, records: Any, **kwargs: Any) -> Any:
        raise ResourceUnavailable("resource backend is disabled")

    async def stats(self, pool: str) -> Any:
        raise ResourceUnavailable("resource backend is disabled")


class _EmbeddedBackend(ResourceBackend):
    """Backend view over a Purser owned by ``ResourceRuntime``."""

    def __init__(
        self,
        settings: Settings,
        *,
        purser: Any,
        pools: Mapping[str, Any],
    ) -> None:
        self.settings = settings
        self._purser = purser
        self._pools = dict(pools)

    async def close(self) -> None:
        # ResourceRuntime owns the Purser lifecycle.
        return None

    async def acquire(
        self,
        pool: str,
        *,
        request: Mapping[str, Any],
    ) -> LeaseHandle:
        from purser import AcquireRequest

        pool_obj = self._pools.get(pool)
        if pool_obj is None:
            raise ResourceUnavailable(f"unknown purser pool {pool!r}")
        raw = await pool_obj.acquire(AcquireRequest(**dict(request)))
        return LeaseHandle(
            name=pool,
            pool=pool,
            raw=raw,
            backend=self,
            use_limit=request.get("use_limit"),
        )

    async def release(
        self,
        handle: LeaseHandle,
        *,
        outcome: ResourceOutcome,
        used: int,
    ) -> bool:
        pool_obj = self._pools.get(handle.pool)
        if pool_obj is None:
            return False
        return await pool_obj.release(
            handle.raw,
            outcome=outcome.value,
            used=used,
        )

    async def renew(self, handle: LeaseHandle) -> bool:
        pool_obj = self._pools.get(handle.pool)
        if pool_obj is None:
            return False
        return await pool_obj.renew_manual(handle.raw)

    async def import_resources(self, pool: str, records: Any, **kwargs: Any) -> Any:
        pool_obj = self._pools.get(pool)
        if pool_obj is None:
            raise ResourceUnavailable(f"unknown purser pool {pool!r}")
        return await pool_obj.import_resources(records, **kwargs)

    async def stats(self, pool: str) -> Any:
        pool_obj = self._pools.get(pool)
        if pool_obj is None:
            raise ResourceUnavailable(f"unknown purser pool {pool!r}")
        return await pool_obj.stats()


class _LocalBackend(ResourceBackend):
    def __init__(
        self,
        address: Any,
        settings: Settings,
        resource_types: dict[str, type] | None = None,
    ) -> None:
        self.address = address
        self.settings = settings
        from purser.local.client import LocalSchedulerClient

        self._client = LocalSchedulerClient(address, resource_types=resource_types)

    async def close(self) -> None:
        self._client.close()

    async def acquire(
        self,
        pool: str,
        *,
        request: Mapping[str, Any],
    ) -> LeaseHandle:
        payload = dict(request)
        payload = {key: value for key, value in payload.items() if value is not None}
        raw = await asyncio.to_thread(
            self._client.acquire,
            pool,
            payload,
            timeout=payload.get("timeout"),
        )
        return LeaseHandle(
            name=pool,
            pool=pool,
            raw=raw,
            backend=self,
            use_limit=request.get("use_limit"),
        )

    async def release(
        self,
        handle: LeaseHandle,
        *,
        outcome: ResourceOutcome,
        used: int,
    ) -> bool:
        return await asyncio.to_thread(
            self._client.release,
            handle.pool,
            handle.token,
            outcome=outcome.value,
            used=used,
        )

    async def renew(self, handle: LeaseHandle) -> bool:
        await asyncio.to_thread(self._client.renew, handle.pool, handle.token)
        return True

    async def import_resources(self, pool: str, records: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(
            self._client.import_resources,
            pool,
            records,
            **kwargs,
        )

    async def stats(self, pool: str) -> Any:
        return await asyncio.to_thread(self._client.stats, pool)


class ResourceManager:
    """Owns the resource backend and exposes the leases used by sessions."""

    def __init__(self, backend: ResourceBackend, settings: Settings) -> None:
        self.backend = backend
        self.settings = settings

    async def start(self) -> None:
        await self.backend.start()

    async def close(self) -> None:
        await self.backend.close()

    async def acquire(
        self,
        pool: str,
        request: Any = None,
        *,
        consumer: str,
        **overrides: Any,
    ) -> LeaseHandle:
        payload = lease_request_payload(request)
        payload.update(overrides)
        payload["consumer"] = consumer
        return await self.backend.acquire(pool, request=payload)

    async def import_resources(self, pool: str, records: Any, **kwargs: Any) -> Any:
        return await self.backend.import_resources(pool, records, **kwargs)

    async def stats(self, pool: str) -> Any:
        return await self.backend.stats(pool)


def make_backend(
    settings: Settings,
    scheduler: Any = None,
    resource_types: dict[str, type] | None = None,
) -> ResourceBackend:
    if settings.purser_mode == "off":
        return _NoopBackend()
    if settings.purser_mode == "local":
        if scheduler is None:
            raise ConfigError("purser_mode='local' requires a started LocalScheduler")
        return _LocalBackend(scheduler.address, settings, resource_types)
    if settings.purser_mode == "embedded":
        raise ConfigError(
            "embedded purser is owned by ResourceRuntime; workers must use "
            "purser_mode='local' or 'off'"
        )
    if settings.purser_mode == "auto":
        raise ConfigError("purser_mode='auto' must be resolved before creating a backend")
    raise ConfigError(f"unknown purser_mode: {settings.purser_mode!r}")
