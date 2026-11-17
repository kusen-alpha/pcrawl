from __future__ import annotations

from .outcomes import ResourceOutcome


class PcrawlError(Exception):
    """Base error for pcrawl."""


class ConfigError(PcrawlError):
    """Invalid settings or a mode combination the runtime cannot support."""


class QueueError(PcrawlError):
    """Task queue operation failed."""


class ResourceError(PcrawlError):
    """Resource backend operation failed."""


class ResourceUnavailable(ResourceError):
    """The requested resource could not be acquired."""


class ResourceInvalid(ResourceError):
    """A resource was acquired but is no longer usable."""

    def __init__(self, outcome: ResourceOutcome, message: str = "") -> None:
        self.outcome = outcome
        super().__init__(message or f"resource is invalid: {outcome.value}")


class IgnoreRequest(PcrawlError):
    """Raised by a middleware or callback to skip a request."""


class CloseSpider(PcrawlError):
    """Request a graceful shutdown from a spider hook or callback.

    ``reason`` is forwarded to ``Spider.close_spider``. The runtime drops the
    task/request that raised it instead of retrying it.
    """

    def __init__(self, reason: str = "cancelled") -> None:
        self.reason = reason
        super().__init__(reason)
