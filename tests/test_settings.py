from __future__ import annotations

import sys
from types import ModuleType

import pytest

from pcrawl import (
    AsyncSpider,
    ConfigError,
    Item,
    MultiProcessSpider,
    MultiThreadSpider,
    Request,
    Response,
    Spider,
)
from pcrawl.cli import _setting_overrides, build_parser
from pcrawl.engine import CrawlerEngine
from pcrawl.middleware import DownloaderMiddlewareManager
from pcrawl.pipeline import PipelineManager
from pcrawl.settings import Settings


class SettingsSpider(Spider):
    name = "settings-spider"
    custom_settings = {
        "CONCURRENT_REQUESTS": 7,
        "USER_AGENT": "settings-spider/1.0",
    }


class LegacySettingsSpider(Spider):
    name = "legacy-settings-spider"
    settings = Settings(workers=3)


def _project_module() -> ModuleType:
    module = ModuleType("pcrawl_test_project_settings")
    module.CONCURRENT_REQUESTS = 5
    module.DOWNLOAD_TIMEOUT = 9
    module.CUSTOM_EXTENSION_KEY = {"enabled": True}
    return module


def test_settings_support_scrapy_accessors_and_extra_keys() -> None:
    settings = Settings(
        DOWNLOAD_TIMEOUT=7,
        RETRY_TIMES=4,
        QUEUE_ORDER="priority",
        CUSTOM_EXTENSION_KEY={"enabled": True},
        KEEP_ALIVE=True,
    )

    assert settings.download_timeout == 7
    assert settings.queue_order == "priority"
    assert settings.request_max_retries == 4
    assert settings.getint("DOWNLOAD_TIMEOUT") == 7
    assert settings.getbool("KEEP_ALIVE") is True
    assert settings.getdict("CUSTOM_EXTENSION_KEY") == {"enabled": True}
    assert settings["CONCURRENT_REQUESTS"] == 1

    settings["CONCURRENT_REQUESTS"] = 12
    assert settings.concurrency == 12
    assert "concurrent_requests" in settings
    assert settings.get("MISSING", "fallback") == "fallback"


def test_settings_loads_project_module_and_config_file(tmp_path, monkeypatch) -> None:
    module = _project_module()
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.delenv("PCRAWL_SETTINGS_MODULE", raising=False)
    config = tmp_path / "pcrawl.cfg"
    config.write_text("[settings]\ndefault = pcrawl_test_project_settings\n", encoding="utf-8")

    settings = Settings.load(config_file=config)

    assert settings.concurrency == 5
    assert settings.download_timeout == 9
    assert settings.getdict("CUSTOM_EXTENSION_KEY") == {"enabled": True}


def test_engine_layers_project_spider_and_explicit_settings() -> None:
    project = _project_module()

    engine = CrawlerEngine(SettingsSpider, settings_module=project)

    assert engine.settings.concurrency == 7
    assert engine.settings.user_agent == "settings-spider/1.0"
    assert engine.settings.download_timeout == 9
    assert engine.spider.settings is engine.settings

    overridden = CrawlerEngine(
        SettingsSpider,
        {"CONCURRENT_REQUESTS": 11},
        settings_module=project,
    )
    assert overridden.settings.concurrency == 11


def test_legacy_settings_object_only_overrides_values_it_sets() -> None:
    project = _project_module()

    engine = CrawlerEngine(LegacySettingsSpider, settings_module=project)

    assert engine.mode == "inline"
    assert engine.settings.workers == 3
    assert engine.settings.concurrency == 5  # still comes from project settings


def test_spider_execution_mode_is_defined_by_class() -> None:
    assert CrawlerEngine(Spider).mode == "inline"
    assert CrawlerEngine(AsyncSpider).mode == "async"
    assert CrawlerEngine(MultiThreadSpider).mode == "thread"
    assert CrawlerEngine(MultiProcessSpider).mode == "process"


def test_execution_mode_cannot_be_configured_through_settings() -> None:
    with pytest.raises(ConfigError):
        CrawlerEngine(AsyncSpider, {"MODE": "inline"})
    with pytest.raises(ConfigError):
        CrawlerEngine(AsyncSpider, Settings(mode="async"))

    project = ModuleType("pcrawl_preset_settings")
    project.MODE = "thread"
    with pytest.raises(ConfigError):
        CrawlerEngine(AsyncSpider, settings_module=project)

    class CustomAsyncSpider(AsyncSpider):
        custom_settings = {"MODE": "inline"}

    with pytest.raises(ConfigError):
        CrawlerEngine(CustomAsyncSpider)


def test_embedded_purser_is_rejected_for_thread_and_process_modes() -> None:
    with pytest.raises(ConfigError):
        CrawlerEngine(MultiThreadSpider, {"purser_mode": "embedded"})
    with pytest.raises(ConfigError):
        CrawlerEngine(MultiProcessSpider, {"purser_mode": "embedded"})


@pytest.mark.asyncio
async def test_middleware_and_pipeline_priorities() -> None:
    calls: list[str] = []

    class FirstMiddleware:
        def process_request(self, request, spider, ctx):
            calls.append("first-request")

        def process_response(self, request, response, spider, ctx):
            calls.append("first-response")
            return response

    class SecondMiddleware:
        def process_request(self, request, spider, ctx):
            calls.append("second-request")

        def process_response(self, request, response, spider, ctx):
            calls.append("second-response")
            return response

    class FirstPipeline:
        def process_item(self, item, spider, ctx):
            calls.append("first-pipeline")
            return item

    class SecondPipeline:
        def process_item(self, item, spider, ctx):
            calls.append("second-pipeline")
            return item

    settings = Settings(
        DOWNLOADER_MIDDLEWARES={FirstMiddleware: 200, SecondMiddleware: 100},
        ITEM_PIPELINES={FirstPipeline: 200, SecondPipeline: 100},
    )
    request = Request("http://example.com/")
    response = Response(request.url, 200, {}, b"ok", request)

    middleware = DownloaderMiddlewareManager(settings)
    await middleware.process_request(request, None, None)
    await middleware.process_response(request, response, None, None)
    pipeline = PipelineManager(settings)
    await pipeline.process_item(Item(value=1), None, None)

    assert calls == [
        "second-request",
        "first-request",
        "first-response",
        "second-response",
        "second-pipeline",
        "first-pipeline",
    ]


def test_cli_set_overrides() -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "run",
            "examples.simple_http:SimpleSpider",
            "-s",
            "CONCURRENT_REQUESTS=4",
            "-s",
            "KEEP_ALIVE=true",
            "-s",
            "CUSTOM_EXTENSION_KEY={'enabled': True}",
        ]
    )

    changes = _setting_overrides(args, parser)

    assert changes["CONCURRENT_REQUESTS"] == 4
    assert changes["KEEP_ALIVE"] is True
    assert changes["CUSTOM_EXTENSION_KEY"] == {"enabled": True}
