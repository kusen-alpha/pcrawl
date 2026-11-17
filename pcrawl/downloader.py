from __future__ import annotations

from typing import Any

from .http import Request, Response
from .settings import Settings


class HttpDownloader:
    """Async HTTP downloader backed by httpx.

    It has no knowledge of proxies, accounts or browsers. Request-scoped
    resources are exposed through ``request.meta`` by ``Spider.bind_resource``
    and can be applied by a downloader middleware or a custom downloader.
    """

    def __init__(self, settings: Settings) -> None:
        import httpx

        self.settings = settings
        self._client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(settings.download_timeout),
            headers={"User-Agent": settings.user_agent},
            trust_env=settings.trust_env,
        )

    async def fetch(self, request: Request) -> Response:
        headers = dict(request.headers)
        body = request.body
        response = await self._client.request(
            request.method,
            request.url,
            headers=headers or None,
            content=body if body is not None else None,
            timeout=request.meta.get("timeout", self.settings.download_timeout),
        )
        return Response(
            url=str(response.url),
            status=response.status_code,
            headers={key: value for key, value in response.headers.items()},
            body=response.content,
            request=request,
            meta=dict(request.meta),
            encoding=response.encoding,
        )

    async def close(self) -> None:
        await self._client.aclose()
