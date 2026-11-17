"""pcrawl: a small Scrapy-inspired crawler runtime with purser leases.

The public surface is intentionally small. A spider subclasses :class:`Spider`
and is executed by :class:`CrawlerEngine`; browser, account and proxy objects
remain user-owned inside ``open_session``/``close_session``.
"""

from .engine import CrawlerEngine
from .exceptions import (
    CloseSpider,
    ConfigError,
    IgnoreRequest,
    PcrawlError,
    QueueError,
    ResourceError,
    ResourceInvalid,
    ResourceUnavailable,
)
from .http import Request, RequestFailure, Response
from .item import Item
from .outcomes import ResourceOutcome
from .queue import (
    MemoryRequestQueue,
    MemoryTaskQueue,
    RedisRequestQueue,
    RedisTaskQueue,
    RequestQueue,
    SharedMemoryRequestQueue,
    SharedMemoryTaskQueue,
    TaskQueue,
)
from .resources import (
    LeaseHandle,
    PoolSpec,
    ResourcePlan,
    ResourceSetup,
    ResourceSpec,
    WorkerSession,
)
from .settings import Settings
from .signals import (
    ITEM_SCRAPED,
    SESSION_CLOSED,
    SESSION_OPENED,
    SPIDER_CLOSED,
    SPIDER_OPENED,
    TASK_FAILED,
    TASK_RECEIVED,
    TASK_SUCCEEDED,
    WORKER_STARTED,
    WORKER_STOPPED,
    SignalManager,
)
from .spider import (
    AsyncSpider,
    MultiProcessSpider,
    MultiThreadSpider,
    ResourceContext,
    Spider,
    SpiderContext,
)
from .stats import Stats
from .task import Task

__version__ = "0.1.0"

__all__ = [
    "AsyncSpider",
    "CloseSpider",
    "ConfigError",
    "CrawlerEngine",
    "ITEM_SCRAPED",
    "IgnoreRequest",
    "Item",
    "LeaseHandle",
    "MemoryRequestQueue",
    "MemoryTaskQueue",
    "MultiProcessSpider",
    "MultiThreadSpider",
    "PcrawlError",
    "PoolSpec",
    "QueueError",
    "RedisRequestQueue",
    "RedisTaskQueue",
    "Request",
    "RequestFailure",
    "RequestQueue",
    "ResourceContext",
    "ResourceError",
    "ResourceInvalid",
    "ResourceOutcome",
    "ResourcePlan",
    "ResourceSetup",
    "ResourceSpec",
    "ResourceUnavailable",
    "Response",
    "SESSION_CLOSED",
    "SESSION_OPENED",
    "SPIDER_CLOSED",
    "SPIDER_OPENED",
    "Settings",
    "SharedMemoryRequestQueue",
    "SharedMemoryTaskQueue",
    "SignalManager",
    "Spider",
    "SpiderContext",
    "Stats",
    "TASK_FAILED",
    "TASK_RECEIVED",
    "TASK_SUCCEEDED",
    "Task",
    "TaskQueue",
    "WORKER_STARTED",
    "WORKER_STOPPED",
    "WorkerSession",
]
