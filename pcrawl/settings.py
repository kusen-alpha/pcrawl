from __future__ import annotations

import configparser
import importlib
import json
import os
from collections.abc import Iterator, Mapping
from copy import copy as shallow_copy
from dataclasses import MISSING, dataclass, field, fields
from pathlib import Path
from types import ModuleType
from typing import Any, Literal

from .exceptions import ConfigError

QueueKind = Literal["memory", "redis"]
QueueOrder = Literal["fifo", "priority"]
PurserMode = Literal["auto", "off", "embedded", "local"]

ENV_SETTINGS_MODULE = "PCRAWL_SETTINGS_MODULE"
DEFAULT_CONFIG_FILE = "pcrawl.cfg"

# Scrapy-style names are accepted in settings modules and ``-s`` overrides.
# Unknown uppercase names are still stored and readable through ``get()``.
_ALIASES = {
    "NAME": "name",
    "WORKERS": "workers",
    "CONCURRENCY": "concurrency",
    "CONCURRENT_REQUESTS": "concurrency",
    "IDLE_TIMEOUT": "idle_timeout",
    "KEEP_ALIVE": "keep_alive",
    "QUEUE_ORDER": "queue_order",
    "REDIS_URL": "redis_url",
    "REDIS_KEY": "redis_key",
    "VISIBILITY_TIMEOUT": "visibility_timeout",
    "CLAIM_POLL_INTERVAL": "claim_poll_interval",
    "MAX_RETRIES": "request_max_retries",
    "RETRY_TIMES": "request_max_retries",
    "RETRY_DELAY": "request_retry_delay",
    "TASK_MAX_RETRIES": "task_max_retries",
    "TASK_RETRY_DELAY": "task_retry_delay",
    "RETRY_HTTP_CODES": "request_retry_http_codes",
    "RETRY_PRIORITY_ADJUST": "request_retry_priority_adjust",
    "RETRY_MAX_DELAY": "request_retry_max_delay",
    "REQUEST_DEDUP_TTL": "request_dedup_ttl",
    "DOWNLOAD_TIMEOUT": "download_timeout",
    "USER_AGENT": "user_agent",
    "TRUST_ENV": "trust_env",
    "SHUTDOWN_TIMEOUT": "shutdown_timeout",
    "DOWNLOADER_MIDDLEWARES": "downloader_middlewares",
    "SPIDER_MIDDLEWARES": "spider_middlewares",
    "ITEM_PIPELINES": "item_pipelines",
    "PURSER_MODE": "purser_mode",
    "PURSER_STORAGE": "purser_storage",
    "PURSER_COORDINATION": "purser_coordination",
    "PURSER_SQLITE_PATH": "purser_sqlite_path",
    "PURSER_REDIS_URL": "purser_redis_url",
    "PURSER_START_METHOD": "purser_start_method",
    "RESOURCE_WAIT_TIMEOUT": "resource_wait_timeout",
    "LEASE_TTL": "lease_ttl",
    "MP_START_METHOD": "mp_start_method",
    "LOG_LEVEL": "log_level",
    "STATS_INTERVAL": "stats_interval",
}

_MISSING = object()


def _normalize_key(name: Any) -> str:
    key = str(name)
    return _ALIASES.get(key.upper(), key)


def _field_names() -> tuple[str, ...]:
    return tuple(
        item.name for item in fields(Settings) if not item.name.startswith("_")
    )


def _settings_module_from_config(path: str | os.PathLike[str] = DEFAULT_CONFIG_FILE) -> str | None:
    config_path = Path(path)
    if not config_path.is_file():
        return None
    parser = configparser.ConfigParser()
    parser.read(config_path, encoding="utf-8")
    return parser.get("settings", "default", fallback=None)


def _module_values(module: str | ModuleType) -> dict[str, Any]:
    if isinstance(module, str):
        module = importlib.import_module(module)
    return {
        name: getattr(module, name)
        for name in dir(module)
        if name.isupper() and not name.startswith("_")
    }


@dataclass(init=False)
class Settings:
    """Runtime settings with Scrapy-like layering and access helpers.

    The dataclass fields are the framework's known configuration. Unknown
    keys are kept as extras so extensions can store their own settings without
    changing this class. ``Settings`` is a mutable mapping-like object while
    the engine is being configured; callers should use ``set()``/``update()``
    so explicit-value tracking keeps layering predictable.
    """

    name: str = "pcrawl"
    workers: int = 1
    concurrency: int = 1
    idle_timeout: float = 2.0
    keep_alive: bool = False

    queue_backend: QueueKind = "memory"
    queue_order: QueueOrder = "fifo"
    redis_url: str = "redis://localhost:6379/0"
    redis_key: str = "pcrawl"
    visibility_timeout: float = 300.0
    claim_poll_interval: float = 0.2

    task_max_retries: int = 3
    task_retry_delay: float = 1.0
    request_max_retries: int = 2
    request_retry_delay: float = 1.0
    request_retry_http_codes: tuple[int, ...] = (429, 500, 502, 503, 504)
    request_retry_priority_adjust: int = -1
    request_retry_max_delay: float = 60.0
    request_dedup_ttl: float = 0.0
    download_timeout: float = 30.0
    user_agent: str = "pcrawl/0.1"
    trust_env: bool = False
    shutdown_timeout: float = 30.0

    downloader_middlewares: Any = ()
    spider_middlewares: Any = ()
    item_pipelines: Any = ()

    purser_mode: PurserMode = "auto"
    purser_storage: str = "memory"
    purser_coordination: str = "memory"
    purser_sqlite_path: str | None = None
    purser_redis_url: str | None = None
    purser_start_method: str = "spawn"
    resource_wait_timeout: float = 30.0
    lease_ttl: float = 60.0

    mp_start_method: str = "spawn"
    log_level: str = "INFO"
    stats_interval: float = 0.0

    _extra: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)
    _explicit: set[str] = field(default_factory=set, repr=False, compare=False)

    def __init__(self, **values: Any) -> None:
        for item in fields(type(self)):
            if item.name.startswith("_"):
                continue
            if item.default is not MISSING:
                setattr(self, item.name, item.default)
            elif item.default_factory is not MISSING:
                setattr(self, item.name, item.default_factory())
        object.__setattr__(self, "_extra", {})
        object.__setattr__(self, "_explicit", set())
        self.update(values)

    # -- Scrapy-like access ------------------------------------------------
    def get(self, name: str, default: Any = None) -> Any:
        key = _normalize_key(name)
        if key in _field_names():
            return getattr(self, key)
        if name in self._extra:
            return self._extra[name]
        return self._extra.get(key, default)

    def getint(self, name: str, default: int | None = None) -> int | None:
        value = self.get(name, default)
        if value is None:
            return None
        return int(value)

    def getfloat(self, name: str, default: float | None = None) -> float | None:
        value = self.get(name, default)
        if value is None:
            return None
        return float(value)

    def getbool(self, name: str, default: bool | None = None) -> bool | None:
        value = self.get(name, default)
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip().lower() in ("1", "true", "yes", "on")
        return bool(value)

    def getlist(self, name: str, default: Any = None) -> list[Any]:
        value = self.get(name, default)
        if value is None:
            return []
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        if isinstance(value, (list, tuple, set, frozenset)):
            return list(value)
        return [value]

    def getdict(self, name: str, default: Any = None) -> dict[str, Any]:
        value = self.get(name, default)
        if value is None:
            return {}
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, str):
            return dict(json.loads(value))
        raise TypeError(f"setting {name!r} is not a mapping")

    def set(self, name: str, value: Any) -> "Settings":
        key = _normalize_key(name)
        if key in _field_names():
            setattr(self, key, value)
        else:
            # Keep unknown keys exactly as supplied; known Scrapy-style
            # aliases were normalized above.
            self._extra[str(name)] = value
        self._explicit.add(key)
        return self

    def update(self, values: Mapping[str, Any] | "Settings" | None = None, **kwargs: Any) -> "Settings":
        if values is not None:
            source = values.to_dict() if isinstance(values, Settings) else values
            for key, value in dict(source).items():
                self.set(str(key), value)
        for key, value in kwargs.items():
            self.set(key, value)
        return self

    def setdefault(self, name: str, default: Any) -> Any:
        value = self.get(name, _MISSING)
        if value is _MISSING:
            self.set(name, default)
            return default
        return value

    def setdict(self, values: Mapping[str, Any]) -> "Settings":
        return self.update(values)

    def setmodule(self, module: str | ModuleType) -> "Settings":
        return self.update(_module_values(module))

    def explicit_values(self) -> dict[str, Any]:
        result = {
            name: getattr(self, name)
            for name in self._explicit
            if name in _field_names()
        }
        result.update(self._extra)
        return result

    def to_dict(self) -> dict[str, Any]:
        result = {name: getattr(self, name) for name in _field_names()}
        result.update(self._extra)
        return result

    def keys(self) -> list[str]:
        return list(_field_names()) + [key for key in self._extra if key not in _field_names()]

    def items(self) -> Iterator[tuple[str, Any]]:
        for key in self.keys():
            yield key, self.get(key)

    def copy(self) -> "Settings":
        return self.replace()

    def replace(self, **changes: Any) -> "Settings":
        new = shallow_copy(self)
        object.__setattr__(new, "_extra", dict(self._extra))
        object.__setattr__(new, "_explicit", set(self._explicit))
        new.update(changes)
        return new

    def __getitem__(self, name: str) -> Any:
        value = self.get(name, _MISSING)
        if value is _MISSING:
            raise KeyError(name)
        return value

    def __setitem__(self, name: str, value: Any) -> None:
        self.set(name, value)

    def __contains__(self, name: object) -> bool:
        if not isinstance(name, str):
            return False
        return _normalize_key(name) in _field_names() or name in self._extra

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    # -- validation / construction ----------------------------------------
    def validate(self) -> None:
        if any(str(key).upper() in ("MODE", "PCRAWL_MODE") for key in self._extra):
            raise ConfigError(
                "execution mode is defined by the Spider class "
                "(Spider/AsyncSpider/MultiThreadSpider/MultiProcessSpider); "
                "remove MODE/mode from settings"
            )
        if self.queue_backend not in ("memory", "redis"):
            raise ConfigError(f"unknown queue_backend: {self.queue_backend!r}")
        if self.queue_order not in ("fifo", "priority"):
            raise ConfigError(f"unknown queue_order: {self.queue_order!r}")
        if self.purser_mode not in ("auto", "off", "embedded", "local"):
            raise ConfigError(f"unknown purser_mode: {self.purser_mode!r}")
        if self.workers < 1:
            raise ConfigError("workers must be >= 1")
        if self.concurrency < 1:
            raise ConfigError("concurrency must be >= 1")
        if self.task_max_retries < 0:
            raise ConfigError("task_max_retries must be >= 0")
        if self.request_max_retries < 0:
            raise ConfigError("request_max_retries must be >= 0")
        if self.request_retry_delay < 0:
            raise ConfigError("request_retry_delay must be >= 0")
        if self.request_retry_max_delay < self.request_retry_delay:
            raise ConfigError(
                "request_retry_max_delay must be >= request_retry_delay"
            )
        if self.request_dedup_ttl < 0:
            raise ConfigError("request_dedup_ttl must be >= 0")

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Settings":
        return cls(**dict(data))

    @classmethod
    def from_module(
        cls,
        module: str | ModuleType,
        *,
        base: "Settings | None" = None,
    ) -> "Settings":
        settings = base.copy() if isinstance(base, Settings) else cls()
        settings.update(_module_values(module))
        return settings

    @classmethod
    def load(
        cls,
        settings_module: str | ModuleType | None = None,
        *,
        overrides: Mapping[str, Any] | "Settings" | None = None,
        base: "Settings | None" = None,
        use_env: bool = True,
        config_file: str | os.PathLike[str] = DEFAULT_CONFIG_FILE,
    ) -> "Settings":
        """Load defaults, project settings, then explicit overrides.

        Project settings come from ``settings_module``, else
        ``PCRAWL_SETTINGS_MODULE``, else the ``[settings] default`` value in
        ``pcrawl.cfg``. Only uppercase names are read from a module.
        """

        settings = base.copy() if isinstance(base, Settings) else cls()
        module = settings_module
        if module is None and use_env:
            module = os.environ.get(ENV_SETTINGS_MODULE) or None
        if module is None:
            module = _settings_module_from_config(config_file) or None
        if module is not None:
            settings.update(_module_values(module))
        if overrides is not None:
            values = overrides.explicit_values() if isinstance(overrides, Settings) else overrides
            settings.update(values)
        return settings
