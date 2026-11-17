from __future__ import annotations

import base64
import json as jsonlib
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.parse import urljoin as _urljoin

from .utils import serialize_callable


@dataclass
class Request:
    """A schedulable HTTP request, modelled after Scrapy's Request.

    A ``Request`` is a unit of work in the request queue, not something a
    spider executes inline. Yield it from ``start_requests`` or a callback and
    the runtime will schedule it independently.
    """

    url: str
    method: str = "GET"
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes | str | None = None
    callback: str | Callable[..., Any] | None = None
    errback: str | Callable[..., Any] | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    cb_kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    dont_filter: bool = False
    route: str = ""
    resource_specs: dict[str, Any] = field(default_factory=dict)
    retries: int = 0
    max_retries: int | None = None
    parent_task_id: str | None = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex)

    def copy(self, **changes: Any) -> "Request":
        data = {
            "url": self.url,
            "method": self.method,
            "headers": dict(self.headers),
            "body": self.body,
            "callback": self.callback,
            "errback": self.errback,
            "meta": dict(self.meta),
            "cb_kwargs": dict(self.cb_kwargs),
            "priority": self.priority,
            "dont_filter": self.dont_filter,
            "route": self.route,
            "resource_specs": dict(self.resource_specs),
            "retries": self.retries,
            "max_retries": self.max_retries,
            "parent_task_id": self.parent_task_id,
            "id": self.id,
        }
        data.update(changes)
        return Request(**data)

    def to_dict(self) -> dict[str, Any]:
        body = self.body
        body_encoding = None
        if isinstance(body, bytes):
            body = base64.b64encode(body).decode("ascii")
            body_encoding = "base64"
        return {
            "id": self.id,
            "url": self.url,
            "method": self.method,
            "headers": dict(self.headers),
            "body": body,
            "body_encoding": body_encoding,
            "callback": serialize_callable(self.callback),
            "errback": serialize_callable(self.errback),
            "meta": dict(self.meta),
            "cb_kwargs": dict(self.cb_kwargs),
            "priority": self.priority,
            "dont_filter": self.dont_filter,
            "route": self.route,
            "resource_specs": dict(self.resource_specs),
            "retries": self.retries,
            "max_retries": self.max_retries,
            "parent_task_id": self.parent_task_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Request":
        body = data.get("body")
        if data.get("body_encoding") == "base64" and isinstance(body, str):
            body = base64.b64decode(body)
        return cls(
            url=str(data["url"]),
            method=str(data.get("method") or "GET"),
            headers=dict(data.get("headers") or {}),
            body=body,
            callback=data.get("callback"),
            errback=data.get("errback"),
            meta=dict(data.get("meta") or {}),
            cb_kwargs=dict(data.get("cb_kwargs") or {}),
            priority=int(data.get("priority") or 0),
            dont_filter=bool(data.get("dont_filter")),
            route=str(data.get("route") or ""),
            resource_specs=dict(
                data.get("resource_specs", data.get("resources")) or {}
            ),
            retries=int(data.get("retries") or 0),
            max_retries=data.get("max_retries"),
            parent_task_id=data.get("parent_task_id") or data.get("task_id"),
            id=str(data.get("id") or uuid.uuid4().hex),
        )


@dataclass
class Response:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
    request: Request
    meta: dict[str, Any] = field(default_factory=dict)
    encoding: str | None = None
    _selector: Any = field(default=None, init=False, repr=False, compare=False)
    _base_url: str | None = field(default=None, init=False, repr=False, compare=False)

    @property
    def text(self) -> str:
        return self.body.decode(self.encoding or "utf-8", errors="replace")

    def json(self) -> Any:
        return jsonlib.loads(self.text)

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 400

    @property
    def selector(self) -> Any:
        if self._selector is None:
            from parsel import Selector

            self._selector = Selector(
                text=self.text,
                base_url=self.base_url,
                type="html",
            )
        return self._selector

    @property
    def base_url(self) -> str:
        if self._base_url is None:
            from parsel import Selector

            href = Selector(text=self.text, type="html").xpath("//base/@href").get()
            self._base_url = _urljoin(self.url, href) if href else self.url
        return self._base_url

    def css(self, query: str) -> Any:
        return self.selector.css(query)

    def xpath(self, query: str) -> Any:
        return self.selector.xpath(query)

    def urljoin(self, url: str) -> str:
        return _urljoin(self.base_url, url)

    def follow(
        self,
        url: str | Request | Any,
        callback: str | Callable[..., Any] | None = None,
        **kwargs: Any,
    ) -> Request:
        """Build a ``Request`` for a link and keep the current callback by default."""

        if isinstance(url, Request):
            request = url
            if callback is not None:
                request.callback = callback
            return request
        if hasattr(url, "get"):
            url = url.get()
        callback = callback or self.request.callback
        changes = {
            "url": self.urljoin(str(url)),
            "callback": callback,
            "meta": dict(self.request.meta),
            "cb_kwargs": dict(self.request.cb_kwargs),
            "method": "GET",
            "route": self.request.route,
            "priority": self.request.priority,
        }
        changes.update(kwargs)
        return Request(**changes)

    def follow_all(
        self,
        urls: Any,
        callback: str | Callable[..., Any] | None = None,
        **kwargs: Any,
    ):
        for url in urls:
            yield self.follow(url, callback=callback, **kwargs)


@dataclass
class RequestFailure:
    """The object passed to ``Request.errback``."""

    request: Request
    exception: BaseException | None = None
    response: Response | None = None
    retries: int = 0
