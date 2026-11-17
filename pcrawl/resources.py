from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from .exceptions import ConfigError, ResourceInvalid, ResourceUnavailable
from .outcomes import ResourceOutcome
from .settings import Settings

logger = logging.getLogger("pcrawl.resources")

ResourceScope = Literal["session", "request"]


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


@dataclass(frozen=True)
class ResourceSpec:
    """How one logical resource should be leased."""

    pool: str
    scope: ResourceScope = "session"
    tags: frozenset[str] = frozenset()
    slots: int = 1
    use_limit: int | None = None
    timeout: float | None = None
    required: bool = True
    required_tag: str | None = None

    def __post_init__(self) -> None:
        if not self.pool:
            raise ConfigError("ResourceSpec.pool cannot be empty")
        if self.scope not in ("session", "request"):
            raise ConfigError(f"unknown resource scope: {self.scope!r}")
        if isinstance(self.tags, str):
            object.__setattr__(self, "tags", frozenset({self.tags}))
        else:
            object.__setattr__(self, "tags", frozenset(self.tags))
        if self.slots < 1:
            raise ConfigError("ResourceSpec.slots must be >= 1")

    @classmethod
    def from_value(cls, value: Any) -> "ResourceSpec":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            return cls(pool=value)
        if isinstance(value, Mapping):
            return cls(**dict(value))
        raise TypeError(f"expected ResourceSpec or mapping, got {type(value)!r}")


@dataclass(frozen=True)
class PoolSpec:
    """Declarative provisioning for one purser pool.

    ``config`` is passed to ``Purser.create_pool``. ``seed`` is imported once
    after the pool exists; each item may be a mapping or a purser ``Resource``
    (including a custom subclass). Custom resource types must register a
    matching adapter through ``ResourceSetup.adapters``. ``import_options`` is
    forwarded to ``import_resources``. Keep this object serializable so the
    engine can use the same declaration in embedded, thread and process modes.
    """

    kind: str
    config: Mapping[str, Any] = field(default_factory=dict)
    seed: tuple[Any, ...] = ()
    import_options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.kind:
            raise ConfigError("PoolSpec.kind cannot be empty")
        object.__setattr__(self, "config", dict(self.config or {}))
        object.__setattr__(self, "import_options", dict(self.import_options or {}))
        raw_seed = self.seed
        if raw_seed is None:
            raw_seed = ()
        elif isinstance(raw_seed, Mapping):
            raw_seed = (raw_seed,)
        object.__setattr__(self, "seed", tuple(raw_seed))

    @classmethod
    def from_value(cls, value: Any) -> "PoolSpec":
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            data = dict(value)
            kind = data.pop("kind", None)
            if not kind:
                raise ConfigError("pool configuration requires a 'kind'")
            seed = data.pop("seed", ())
            import_options = data.pop("import_options", {})
            config = dict(data.pop("config", {}) or {})
            config.update(data)
            return cls(
                kind=str(kind),
                config=config,
                seed=seed,
                import_options=import_options,
            )
        raise TypeError(f"expected PoolSpec or mapping, got {type(value)!r}")


@dataclass(frozen=True)
class ResourceSetup:
    """Mode-neutral resource declaration returned by ``start_resources``.

    The engine owns lifecycle and picks the concrete backend:
    ``inline``/``async`` use an embedded purser, while ``thread``/``process``
    use a parent-owned ``LocalScheduler``. ``adapters`` are registered before
    pools start, so ``PoolSpec.seed`` may contain custom ``Resource`` types.
    User code never receives or passes live ``Purser``/``ResourcePool``
    objects.
    """

    pools: Mapping[str, PoolSpec] = field(default_factory=dict)
    storage: str | None = None
    coordination: str | None = None
    sqlite_path: str | None = None
    redis_url: str | None = None
    adapters: tuple[Any, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "pools",
            {
                str(name): PoolSpec.from_value(value)
                for name, value in dict(self.pools or {}).items()
            },
        )
        adapters = self.adapters
        if adapters is None:
            adapters = ()
        elif isinstance(adapters, (str, bytes)):
            adapters = (adapters,)
        else:
            try:
                adapters = tuple(adapters)
            except TypeError:
                adapters = (adapters,)
        object.__setattr__(self, "adapters", adapters)

    @classmethod
    def from_value(cls, value: Any) -> "ResourceSetup":
        if value is None:
            return cls()
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            return cls(**dict(value))
        raise TypeError(
            "start_resources must return ResourceSetup or a mapping; "
            "live Purser/ResourcePool objects are not supported"
        )


@dataclass
class ResourcePlan:
    """Logical name -> resource specification."""

    specs: dict[str, ResourceSpec] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.specs = {
            str(name): ResourceSpec.from_value(value)
            for name, value in dict(self.specs).items()
        }

    def session_specs(self) -> dict[str, ResourceSpec]:
        return {name: spec for name, spec in self.specs.items() if spec.scope == "session"}

    def request_specs(self) -> dict[str, ResourceSpec]:
        return {name: spec for name, spec in self.specs.items() if spec.scope == "request"}

    @classmethod
    def from_value(cls, value: Any) -> "ResourcePlan":
        if value is None:
            return cls()
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            return cls(dict(value))
        raise TypeError(f"resources must be a ResourcePlan or mapping, got {type(value)!r}")


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

    def set_outcome(self, outcome: ResourceOutcome) -> None:
        self.outcome = outcome

    async def renew(self) -> bool:
        if self.released:
            return False
        return await self._backend.renew(self)

    async def release(
        self,
        outcome: ResourceOutcome | None = None,
        used: int | None = None,
    ) -> bool:
        if self.released:
            return True
        if outcome is not None:
            self.outcome = outcome
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
        plan: ResourcePlan,
        worker_id: str,
    ) -> None:
        self.id = uuid.uuid4().hex
        self.manager = manager
        self.plan = plan
        self.worker_id = worker_id
        self.leases: dict[str, LeaseHandle] = {}
        self.user: dict[str, Any] = {}
        self.outcome = ResourceOutcome.OK
        self.heartbeat_error: BaseException | None = None
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
        name: str,
        spec: ResourceSpec | Mapping[str, Any] | None = None,
        **overrides: Any,
    ) -> LeaseHandle:
        if name in self.leases:
            return self.leases[name]
        if spec is None:
            spec = self.plan.specs.get(name)
        if spec is None:
            raise ConfigError(f"no resource spec for {name!r}")
        spec = ResourceSpec.from_value(spec)
        handle = await self.manager.acquire(
            name,
            spec,
            consumer=f"{self.worker_id}:{self.id}",
            **overrides,
        )
        self.leases[name] = handle
        return handle

    async def release(
        self,
        name: str,
        outcome: ResourceOutcome | None = None,
        used: int | None = None,
    ) -> bool:
        handle = self.leases.pop(name, None)
        if handle is None:
            return True
        return await handle.release(outcome=outcome, used=used)

    def invalidate(self, outcome: ResourceOutcome) -> None:
        self.outcome = outcome
        for handle in self.leases.values():
            handle.set_outcome(outcome)

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

    async def release_all(self, outcome: ResourceOutcome | None = None) -> None:
        await self.stop_heartbeat()
        final_outcome = outcome or self.outcome
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
        name: str,
        tags: frozenset[str],
        slots: int,
        use_limit: int | None,
        timeout: float | None,
        consumer: str,
        resource_id: str | None = None,
        required_tag: str | None = None,
        **kwargs: Any,
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
    async def acquire(self, pool: str, **kwargs: Any) -> LeaseHandle:
        raise ResourceUnavailable(
            "resource backend is off; return ResourceSetup from start_resources "
            "or set purser_mode='embedded'/'local'"
        )

    async def release(self, handle: LeaseHandle, *, outcome: ResourceOutcome, used: int) -> bool:
        return True

    async def import_resources(self, pool: str, records: Any, **kwargs: Any) -> Any:
        raise ResourceUnavailable("resource backend is disabled")

    async def stats(self, pool: str) -> Any:
        raise ResourceUnavailable("resource backend is disabled")


class _EmbeddedBackend(ResourceBackend):
    def __init__(
        self,
        settings: Settings,
        setup: ResourceSetup | None = None,
    ) -> None:
        self.settings = settings
        self.setup = setup or ResourceSetup()
        self._purser: Any = None
        self._pools: dict[str, Any] = {}

    def _pool_specs(self) -> dict[str, PoolSpec]:
        if self.setup.pools:
            return dict(self.setup.pools)
        return {
            name: PoolSpec.from_value(value)
            for name, value in self.settings.purser_pools.items()
        }

    async def start(self) -> None:
        from purser import Purser

        setup = self.setup
        self._purser = Purser(
            storage=setup.storage or self.settings.purser_storage,
            coordination=setup.coordination or self.settings.purser_coordination,
            sqlite_path=setup.sqlite_path or self.settings.purser_sqlite_path,
            redis_url=setup.redis_url or self.settings.purser_redis_url,
        )
        for adapter in setup.adapters:
            self._purser.register_adapter(adapter)
        await self._purser.start()
        for name, spec in self._pool_specs().items():
            config = dict(spec.config)
            config.setdefault("lease_ttl", self.settings.lease_ttl)
            self._pools[name] = self._purser.create_pool(
                name,
                spec.kind,
                config=config,
            )

    async def close(self) -> None:
        if self._purser is not None:
            await self._purser.close()
            self._purser = None
            self._pools.clear()

    async def acquire(
        self,
        pool: str,
        *,
        name: str,
        tags: frozenset[str],
        slots: int,
        use_limit: int | None,
        timeout: float | None,
        consumer: str,
        resource_id: str | None = None,
        required_tag: str | None = None,
        **kwargs: Any,
    ) -> LeaseHandle:
        from purser.core.resource import AcquireRequest

        pool_obj = self._pools.get(pool)
        if pool_obj is None:
            raise ResourceUnavailable(f"unknown purser pool {pool!r}")
        request = AcquireRequest(
            consumer=consumer,
            tags=tags,
            slots=slots,
            use_limit=use_limit,
            timeout=timeout,
            resource_id=resource_id,
            required_tag=required_tag,
            **kwargs,
        )
        raw = await pool_obj.acquire(request)
        return LeaseHandle(
            name=name,
            pool=pool,
            raw=raw,
            backend=self,
            use_limit=use_limit,
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
        name: str,
        tags: frozenset[str],
        slots: int,
        use_limit: int | None,
        timeout: float | None,
        consumer: str,
        resource_id: str | None = None,
        required_tag: str | None = None,
        **kwargs: Any,
    ) -> LeaseHandle:
        request = {
            "consumer": consumer,
            "tags": sorted(tags),
            "slots": slots,
            "use_limit": use_limit,
            "timeout": timeout,
            "resource_id": resource_id,
            "required_tag": required_tag,
            **kwargs,
        }
        request = {key: value for key, value in request.items() if value is not None}
        raw = await asyncio.to_thread(
            self._client.acquire,
            pool,
            request,
            timeout=timeout,
        )
        return LeaseHandle(
            name=name,
            pool=pool,
            raw=raw,
            backend=self,
            use_limit=use_limit,
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
        name: str,
        spec: ResourceSpec,
        *,
        consumer: str,
        **overrides: Any,
    ) -> LeaseHandle:
        return await self.backend.acquire(
            spec.pool,
            name=name,
            tags=spec.tags,
            slots=spec.slots,
            use_limit=spec.use_limit,
            timeout=spec.timeout,
            consumer=consumer,
            resource_id=overrides.pop("resource_id", None),
            required_tag=overrides.pop("required_tag", spec.required_tag),
            **overrides,
        )

    async def import_resources(self, pool: str, records: Any, **kwargs: Any) -> Any:
        return await self.backend.import_resources(pool, records, **kwargs)

    async def stats(self, pool: str) -> Any:
        return await self.backend.stats(pool)


def make_backend(
    settings: Settings,
    scheduler: Any = None,
    setup: ResourceSetup | None = None,
    resource_types: dict[str, type] | None = None,
) -> ResourceBackend:
    if settings.purser_mode == "off":
        return _NoopBackend()
    if settings.purser_mode == "embedded":
        return _EmbeddedBackend(settings, setup)
    if settings.purser_mode == "local":
        if scheduler is None:
            raise ConfigError("purser_mode='local' requires a started LocalScheduler")
        if resource_types is None and setup is not None:
            resource_types = resource_types_from_adapters(setup.adapters)
        return _LocalBackend(scheduler.address, settings, resource_types)
    if settings.purser_mode == "auto":
        raise ConfigError("purser_mode='auto' must be resolved before creating a backend")
    raise ConfigError(f"unknown purser_mode: {settings.purser_mode!r}")
