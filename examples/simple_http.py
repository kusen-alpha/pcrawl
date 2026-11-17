from __future__ import annotations

from pcrawl import AsyncSpider, Item, Response, Settings


class SimpleSpider(AsyncSpider):
    """The smallest Scrapy-shaped spider: URLs in, items out."""

    name = "simple-http"
    start_urls = ("https://example.com/",)
    settings = Settings(concurrency=2, download_timeout=10)

    def parse(self, response: Response):
        yield Item(
            url=response.url,
            status=response.status,
            title=response.css("title::text").get("").strip(),
        )
