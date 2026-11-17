# 第一个 Spider

## 1. 写一个 Spider

```python title="spiders.py"
from pcrawl import AsyncSpider, Item, Response


class SimpleSpider(AsyncSpider):
    name = "simple"
    start_urls = ("https://example.com/",)

    def parse(self, response: Response):
        yield Item(
            url=response.url,
            status=response.status,
            title=response.css("title::text").get("").strip(),
        )
```

`start_urls` 会走完整默认链路：基类 `start_tasks` 把每个 URL 变成 `Task(payload={"url": url})`，基类 `start_requests` 再把任务变成 `Request`。

## 2. 运行

CLI：

```bash
pcrawl run spiders:SimpleSpider --concurrency 4 --json
```

Python：

```python
from pcrawl import CrawlerEngine
from spiders import SimpleSpider

stats = CrawlerEngine(SimpleSpider, {"concurrency": 4}).run()
print(stats["item_scraped"])
```

`run()` 阻塞到所有 worker 结束，返回聚合后的统计字典。CLI 的 `--json` 会把它完整打印出来。

一个成功请求的统计大致是：

```json
{
  "item_scraped": 1,
  "request_received": 1,
  "request_succeeded": 1,
  "task_received": 1,
  "task_succeeded": 1
}
```

## 3. 不需要 HTTP 的采集

`start_requests(task, ctx)` 可以直接产出 `Item`，也可以继续产出后续 `Task`：

```python
from pcrawl import AsyncSpider, Item, Task


class LocalSpider(AsyncSpider):
    name = "local"

    def start_tasks(self):
        for index in range(100):
            yield Task(payload={"row": index})

    def start_requests(self, task, ctx):
        row = task.payload["row"]
        yield Item(index=row, worker=ctx.worker_id)
```

这条路径不创建下载器请求，但 Task 队列、重新入队、Pipeline 和统计仍然生效。

## 4. 换一种执行模式

执行模式由 Spider 类决定，不能通过 Settings 或 CLI 覆盖：

```python
from pcrawl import MultiProcessSpider


class ProcessSpider(MultiProcessSpider, SimpleSpider):
    pass
```

`ProcessSpider.execution_mode == "process"`。混入模式时**模式基类必须放在最左侧**，否则会继承到 `SimpleSpider` 的 `async`。多进程模式要求共享内存或 Redis 队列；默认会使用 `multiprocessing.Manager`。

## 下一步

- 从数据库、文件或 API 灌任务：[任务与请求](../guides/tasks-and-requests.md)
- 用账号、代理或浏览器：[资源会话](../guides/resources.md)
- 写中间件、Pipeline、落库：[中间件与 Pipeline](../guides/components.md)
