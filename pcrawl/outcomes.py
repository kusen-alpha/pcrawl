from __future__ import annotations

from enum import Enum


class ResourceOutcome(str, Enum):
    """How a leased resource was used.

    The values mirror ``purser.core.resource.UsageOutcome``; backends forward
    ``outcome.value`` directly and purser accepts the string form.
    """

    OK = "ok"
    CAPTCHA = "captcha"
    RATE_LIMITED = "rate_limited"
    BANNED = "banned"
    TIMEOUT = "timeout"
    NETWORK_ERROR = "network_error"
    QUOTA_EXHAUSTED = "quota_exhausted"
    PREEMPTED = "preempted"
