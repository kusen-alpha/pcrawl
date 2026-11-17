from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pytest

from pcrawl import (
    AsyncSpider,
    CrawlerEngine,
    Item,
    PoolSpec,
    ResourceSetup,
    ResourceSpec,
    Settings,
    Task,
)

purser_resources = pytest.importorskip("purser.core.resource")
purser_adapters = pytest.importorskip("purser.adapters.base")

Resource = purser_resources.Resource
PayloadField = purser_resources.PayloadField
ResourceAdapter = purser_adapters.ResourceAdapter


@dataclass
class DeviceResource(Resource):
    kind: str = "device"
    serial: Annotated[str | None, PayloadField(required=True)] = None
    region: Annotated[str | None, PayloadField()] = None


class DeviceAdapter(ResourceAdapter):
    kind = "device"
    resource_class = DeviceResource


class CustomResourceSpider(AsyncSpider):
    name = "custom-resource"
    count = 3
    resources = {"device": ResourceSpec(pool="device", use_limit=10)}

    async def start_resources(self):
        return ResourceSetup(
            pools={
                "device": PoolSpec(
                    kind="device",
                    config={"max_slots": 1},
                    seed=[
                        DeviceResource(
                            id="device-1",
                            kind="device",
                            serial="serial-1",
                            region="cn",
                        ),
                        {"id": "device-2", "serial": "serial-2", "region": "us"},
                    ],
                    import_options={"import_validate": "on_import"},
                )
            },
            adapters=(DeviceAdapter,),
        )

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    def open_session(self, session):
        device = session.get("device")
        assert type(device.resource).__name__ == "DeviceResource"
        session.user["device_id"] = device.resource_id
        session.user["device_serial"] = device.payload["serial"]

    def start_requests(self, task, ctx):
        ctx.get_lease("device").mark_used()
        yield Item(
            index=task.payload["index"],
            device=ctx.user["device_id"],
            serial=ctx.user["device_serial"],
        )


class InlineCustomResourceSpider(CustomResourceSpider):
    execution_mode = "inline"


class ThreadCustomResourceSpider(CustomResourceSpider):
    execution_mode = "thread"


class ProcessCustomResourceSpider(CustomResourceSpider):
    execution_mode = "process"


@pytest.mark.parametrize(
    "spider_cls,extra",
    [
        (InlineCustomResourceSpider, {}),
        (CustomResourceSpider, {"concurrency": 2}),
        (ThreadCustomResourceSpider, {"workers": 2}),
        (ProcessCustomResourceSpider, {"workers": 2}),
    ],
)
def test_start_resources_supports_custom_resource(spider_cls, extra):
    settings = Settings(
        idle_timeout=0.15,
        claim_poll_interval=0.01,
        shutdown_timeout=10,
        resource_wait_timeout=8,
        **extra,
    )
    stats = CrawlerEngine(spider_cls, settings).run()

    assert stats["task_received"] == CustomResourceSpider.count
    assert stats["item_scraped"] == CustomResourceSpider.count
