# 信号与统计

pcrawl 提供两个观测面：`Stats` 计数器与 `SignalManager` 事件。它们都不依赖外部监控系统，接入 Prometheus、日志或告警由你在钩子里完成。

## 统计计数器

`Stats` 是线程安全的小型计数器，支持 `inc`、`set`、`get`、`snapshot`、`merge`：

```python
from pcrawl import CrawlerEngine, Stats

stats = Stats()
engine = CrawlerEngine(MySpider, stats=stats)
result = engine.run()          # 无注入时引擎自己创建并返回快照
print(stats.snapshot())
```

`engine.run()` / `run_async()` 的返回值就是最终 `Stats.snapshot()`：

```python
{
    "task_received": 100,
    "task_succeeded": 100,
    "task_retried": 0,
    "task_dropped": 0,
    "task_ignored": 0,
    "request_received": 523,
    "request_succeeded": 510,
    "request_failed": 3,
    "request_retried": 10,
    "request_ignored": 0,
    "request_filtered": 7,
    "item_scraped": 498,
}
```

| 计数器 | 含义 |
| --- | --- |
| `task_received` | worker 从任务队列取到 task |
| `task_succeeded` | `start_requests` 正常返回并 ack |
| `task_retried` | task 失败后放回队列等待重试 |
| `task_dropped` | task 超过最大重试次数并 ack |
| `task_ignored` | task 的 `start_requests` 抛 `IgnoreRequest` |
| `request_received` | worker 取到 request |
| `request_succeeded` | 下载与回调都完成并 ack |
| `request_failed` | 最终失败（errback 处理或超过重试） |
| `request_retried` | request 放回队列等待重试 |
| `request_ignored` | 请求阶段抛 `IgnoreRequest` |
| `request_filtered` | 请求被去重过滤，或回调 yield 的请求被过滤 |
| `item_scraped` | Item 经过全部 Pipeline 后计数 |

注意：

- 没有 `task_failed` 计数器；task 失败通过 `task_retried` / `task_dropped` 和 `TASK_FAILED` 信号观测。
- HTTP 状态码在 `request_retry_http_codes` 内时，先走重试逻辑，最终失败才计 `request_failed`。
- 下载异常若被中间件恢复为 `Response`，不会计 `request_failed`。
- `Stats` 只存在于当前进程；`process` 模式下每个子进程各有一份，父进程在子进程退出时通过队列合并一次，没有实时汇总。

在 Spider 内可以直接使用上下文里的计数器：

```python
def parse(self, response, ctx):
    ctx.stats.inc("pages_with_items")
    yield Item(url=response.url)
```

## 信号

信号处理器按连接顺序执行，支持同步或异步函数。处理器异常只记录日志，不会中断采集：

```python
from pcrawl import (
    CrawlerEngine,
    ITEM_SCRAPED,
    SESSION_OPENED,
    SPIDER_CLOSED,
    SPIDER_OPENED,
    TASK_FAILED,
)

engine = CrawlerEngine(MySpider)


def on_open(spider):
    print("opened", spider.name)


def on_item(item, worker_id):
    print(worker_id, item.get("url"))


def on_task_failed(task, exception, worker_id):
    print("task failed", task.id, type(exception).__name__)


engine.signals.connect(SPIDER_OPENED, on_open)
engine.signals.connect(ITEM_SCRAPED, on_item)
engine.signals.connect(TASK_FAILED, on_task_failed)
```

处理器按关键字接收参数，签名必须接受对应信号传入的键：

| 信号 | 参数 |
| --- | --- |
| `SPIDER_OPENED` | `spider` |
| `SPIDER_CLOSED` | `spider`, `stats` |
| `WORKER_STARTED` | `worker_id`, `spider` |
| `WORKER_STOPPED` | `worker_id`, `spider` |
| `SESSION_OPENED` | `worker_id`, `session` |
| `SESSION_CLOSED` | `worker_id`, `session` |
| `TASK_RECEIVED` | `task`, `worker_id` |
| `TASK_SUCCEEDED` | `task`, `worker_id` |
| `TASK_FAILED` | `task`, `exception`, `worker_id` |
| `ITEM_SCRAPED` | `item`, `worker_id` |

`disconnect(signal, handler)` 可移除处理器。也可以在组件里直接使用上下文：

```python
async def process_item(self, item, spider, ctx):
    ctx.stats.inc("pipeline_items")
    await ctx.signals.send("custom_event", item=item)
```

`ctx.signals` 与 `ctx.stats` 指向当前 worker 的实例，适合组件内部埋点。

### 各模式的可见性

| 模式 | 统计 | 信号 |
| --- | --- | --- |
| `inline` / `async` | 与引擎共享 | 与引擎共享 |
| `thread` | 线程安全共享 | 与引擎共享，处理器可能并发执行 |
| `process` | 子进程结束时合并到父进程 | **子进程内不调用父进程连接的处理器** |

`process` 模式下 `SPIDER_OPENED` / `SPIDER_CLOSED` 仍在父进程触发；`WORKER_*`、`SESSION_*`、`TASK_*`、`ITEM_SCRAPED` 发生在子进程内，父进程注册的处理器收不到。需要跨进程观测时，用 Redis/消息队列或日志落盘，而不是依赖 `SignalManager`。

## 日志

| logger | 内容 |
| --- | --- |
| `pcrawl.engine` | 资源后端、队列、supervisor 生命周期 |
| `pcrawl.runtime` | task/request 重试与丢弃 |
| `pcrawl.queue` | 队列异常 |
| `pcrawl.resources` | 租约、心跳、资源异常 |
| `pcrawl.signals` | 处理器失败 |
| `pcrawl.middleware` / `pcrawl.pipeline` | 组件失败 |
| `pcrawl.spider.<name>` | `Spider.logger` |

`log_level` 控制根日志级别；CLI 会配置带进程号的格式。以库方式运行时请自行配置 logging：

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(process)d] %(name)s: %(message)s",
)
```

`stats_interval` 字段目前保留但未实现周期打印；需要定时观测请在自己的信号处理器或外部任务中读取 `stats.snapshot()`。
