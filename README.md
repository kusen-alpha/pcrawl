# pcrawl

一个受 Scrapy 组件模型启发、原生支持 purser 资源租约的通用采集运行时。

pcrawl 只提供**框架能力**：执行模式、任务/请求队列、下载器、中间件、Pipeline、信号、统计和资源租约托管。浏览器对象、账号对象、代理客户端等具体资源怎么创建和使用，全部由 `Spider` 子类决定。

## 核心模型

```
start_urls ──> 基类 start_tasks ──> TaskQueue ──┐
                                                 ├─ round-robin claim ──> WorkerRuntime
重写 start_requests() ───────────> RequestQueue ─┘
                                                 │
                                                 ├─ WorkerSession（session 级资源租约 + 用户对象）
                                                 └─ RequestLeaseGroup（request 级资源租约）
```

* `Task`：持久化的业务单位，只保存 `payload`、`route`、`priority` 等业务字段，不含 HTTP 字段。
* `Request` / `Response`：独立调度、独立重试的 HTTP 单位，命名和用法与 Scrapy 对齐。
* `TaskQueue` / `RequestQueue`：两类队列各自消费；worker 在两者之间轮转，任务与请求互不阻塞。
* `start_urls` 默认走完整链路：基类 `start_tasks` 生成 `Task`，基类 `start_requests` 再生成 `Request`。只有重写 `start_tasks` / `start_requests` 时，对应阶段才走子类逻辑。
* `ResourceRuntime`：`start_resources(resources)` 收到的 purser 形状门面，负责 adapter、建池和导入；不声明“资源是什么”。
* `WorkerSession`：一次 worker 会话的租约集合，加上 `session.user` 供 Spider 保存自己的浏览器/账号对象；回调中主要使用 `ctx.get_lease()` 和 `ctx.user`。
* `LeaseHandle`：框架托管的租约视图，提供 `payload`、`resource`、`mark_used()`、`renew()`、`release()`。

## 安装

要求 Python 3.10+。

```bash
# purser 与本项目同级时
pip install -e ../purser
pip install -e .[dev]
```

没有 purser 也可以运行普通 HTTP 采集；`purser_mode="auto"` 在没有资源池时会自动关闭资源后端。

## 配置

`Settings` 保留框架的已知字段，同时兼容 Scrapy 风格的动态配置键和访问方法：

```python
from pcrawl import Settings

settings = Settings(
    DOWNLOAD_TIMEOUT=10,                    # Scrapy 风格名称 -> download_timeout
    CUSTOM_EXTENSION_KEY={"enabled": True}, # 任意扩展键
)

settings.getint("CONCURRENT_REQUESTS")     # 1
settings.getbool("KEEP_ALIVE")             # False
settings.getlist("SPIDER_MIDDLEWARES")     # []
settings.getdict("CUSTOM_EXTENSION_KEY")   # {"enabled": True}
settings.get("CUSTOM_EXTENSION_KEY")       # {"enabled": True}
settings["CUSTOM_EXTENSION_KEY"]           # {"enabled": True}
```

常用 Scrapy 名称映射：

| Scrapy 风格 | pcrawl 字段 |
| --- | --- |
| `QUEUE_ORDER` | `queue_order` |
| `CONCURRENT_REQUESTS` | `concurrency` |
| `RETRY_TIMES` / `MAX_RETRIES` | `request_max_retries` |
| `RETRY_DELAY` | `request_retry_delay` |
| `RETRY_HTTP_CODES` | `request_retry_http_codes` |
| `RETRY_PRIORITY_ADJUST` | `request_retry_priority_adjust` |
| `RETRY_MAX_DELAY` | `request_retry_max_delay` |
| `TASK_MAX_RETRIES` | `task_max_retries` |
| `TASK_RETRY_DELAY` | `task_retry_delay` |
| `REQUEST_DEDUP_TTL` | `request_dedup_ttl` |
| `DOWNLOAD_TIMEOUT` | `download_timeout` |
| `USER_AGENT` | `user_agent` |
| `DOWNLOADER_MIDDLEWARES` | `downloader_middlewares` |
| `SPIDER_MIDDLEWARES` | `spider_middlewares` |
| `ITEM_PIPELINES` | `item_pipelines` |
| `LOG_LEVEL` | `log_level` |

项目级配置可以放在 settings 模块中，模块内只有全大写名称会被读取：

```python
# myproject/settings.py
CONCURRENT_REQUESTS = 16
DOWNLOAD_TIMEOUT = 15
DOWNLOADER_MIDDLEWARES = {
    "myproject.middleware:ProxyMiddleware": 500,
}
CUSTOM_EXTENSION_KEY = "project-level"
```

通过 `PCRAWL_SETTINGS_MODULE=myproject.settings` 或 `pcrawl.cfg` 指定：

```ini
# pcrawl.cfg
[settings]
default = myproject.settings
```

Spider 可以用 `custom_settings` 做局部覆盖：

```python
class MySpider(Spider):
    custom_settings = {
        "CONCURRENT_REQUESTS": 4,
        "ITEM_PIPELINES": {"myproject.pipelines:Store": 300},
    }
```

配置优先级从低到高为：

1. `Settings` 默认值；
2. 项目 settings 模块（`PCRAWL_SETTINGS_MODULE` / `pcrawl.cfg`）；
3. `Spider.settings`（兼容旧写法，按显式赋值的字段合并）；
4. `Spider.custom_settings`；
5. `CrawlerEngine(..., settings)` 与 CLI `-s` 覆盖。

执行模式不在这套配置里：它由 `Spider` 子类的 `execution_mode` 决定，`Settings` 和 CLI 都不能覆盖。

```bash
pcrawl run myproject.spiders:MySpider \
  --settings-module myproject.settings \
  -s CONCURRENT_REQUESTS=8 \
  -s DOWNLOAD_TIMEOUT=15
```

`-s/--set` 的值会先按 Python 字面量解析，因此可以直接写数字、列表和字典；字符串不需要额外引号。

## Spider 执行模式

执行模式由 Spider 类唯一决定，不通过 `Settings` 或 CLI 配置：

| 类 | 模式 | 说明 |
| --- | --- | --- |
| `Spider` | `inline` | 同步风格，单 event loop；同步回调可用，但阻塞调用会阻塞 worker |
| `AsyncSpider` | `async` | 异步回调与可配置并发 |
| `MultiThreadSpider` | `thread` | 多线程 worker；多个线程共享一个 Spider 实例 |
| `MultiProcessSpider` | `process` | 多进程 worker；每个子进程创建自己的 Spider 实例 |

```python
from pcrawl import AsyncSpider


class MyAsyncSpider(AsyncSpider):
    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
    }
```

要切换执行方式，直接继承对应的 Spider 类，例如把 `AsyncSpider` 换成 `MultiThreadSpider`。默认 `Spider` 就是同步入口，`inline` 模式仍运行在 asyncio 事件循环上。`MultiProcessSpider` 要求共享内存或 Redis 队列。资源后端默认 `purser_mode="auto"`，线程/进程模式会自动切换到 `LocalScheduler`，用户不需要改 `start_resources`。

## 最小 Spider

```python
from pcrawl import AsyncSpider, Item, Settings


class SimpleSpider(AsyncSpider):
    name = "simple"
    start_urls = ("https://example.com/",)
    settings = Settings(concurrency=4)

    async def parse(self, response):
        yield Item(
            url=response.url,
            status=response.status,
            title=response.css("title::text").get(),
        )
```

```bash
pcrawl run examples.simple_http:SimpleSpider --concurrency 4
```

`start_urls` 默认走两段基类逻辑：基类 `start_tasks` 为每个 URL 生成 `Task(payload={"url": url})` 入任务队列，worker 领取后再由基类 `start_requests` 生成 `Request` 入请求队列。只有重写了这两个方法，才走子类自己的逻辑。回调既可以是同步函数，也可以是 `async def`；声明 `(self, response)` 或 `(self, response, ctx)` 都可以，引擎按签名注入 `ctx`。

`Response` 内置 parsel 选择器，并自动识别页面 `<base href>`：

```python
def parse(self, response):
    yield response.follow(response.css("a.next::attr(href)").get(), callback=self.parse_page)

def parse_page(self, response):
    yield Item(title=response.css("h1::text").get())
    yield from response.follow_all(
        response.css("a.detail::attr(href)").getall(),
        callback=self.parse_detail,
    )

def parse_detail(self, response):
    yield Item(url=response.url)
```

`response.follow(...)` 默认沿用当前请求的 callback、`meta`、`cb_kwargs`、`route` 和 `priority`；`follow_all(...)` 返回请求迭代器。请求的 callback/errback 也可以是字符串方法名或 `module:function` 路径，便于跨进程队列序列化。


## Spider 生命周期

`Spider` 提供两级生命周期钩子：

| 钩子 | 调用时机 | 范围 |
| --- | --- | --- |
| `open_spider()` | 资源启动完成、`start_tasks` 之前 | 每次运行一次，父进程 |
| `close_spider(reason)` | worker 结束、队列和资源关闭之前 | 每次运行一次，父进程 |
| `open_session(session)` | 每个 worker 取得 session 资源之后 | 每个 worker 会话 |
| `close_session(session, reason)` | session 释放资源之前 | 每个 worker 会话 |

`close_spider` 的 `reason` 在正常结束时为 `"finished"`，运行抛出异常时为 `"error"`；在回调、`start_requests` 或 Item Pipeline 中抛出 `CloseSpider("reason")` 时为该自定义值。`CloseSpider` 会让所有 worker 优雅停止，当前 task/request 直接丢弃而不重试。两个 Spider 级钩子都支持同步或 `async def` 实现。进程模式下子进程会创建自己的 Spider 实例，但不会重复执行 `open_spider` / `close_spider`；worker 本地状态请放在 `open_session` / `session.user`。

```python
class MySpider(AsyncSpider):
    async def open_spider(self):
        self.task_db = await connect_task_db()

    def start_tasks(self):
        yield from self.task_db.iter_open_tasks()

    async def close_spider(self, reason):
        await self.task_db.close()
```

## 任务与请求

pcrawl 把“业务任务”和“HTTP 请求”拆成两层独立队列：

```text
start_urls
   │ 基类 start_tasks：每个 URL -> Task(payload={"url": url})
   ▼
TaskQueue ──claim Task──> start_requests(task, ctx) ──> RequestQueue
                                                          │
                                      claim Request -> 下载 -> callback / errback
```

* `Task`：只承载业务字段（`payload`、`route`、`priority`、重试计数），适合从数据库、文件、API 读取的批量工作。
* `Request`：独立的 HTTP 调度单位，支持 `priority`、`route`、`resources`、`callback`、`errback`、`cb_kwargs`、`dont_filter` 和独立重试。
* 两类队列由 worker 轮转消费：任务积压不会阻塞请求下载，请求积压也不会阻塞任务转换；多个 worker 下两者可以并行推进。

### 生产入口

* `start_urls`：基类 `start_tasks` 为每个 URL 生成 `Task(payload={"url": url})` 入任务队列；基类 `start_requests` 再把它变成 `Request` 入请求队列。
* `start_tasks(ctx)`：重写后替代基类任务来源；只 yield `Task`，yield `Request` 会明确报错。
* `start_requests(task, ctx)`：重写后替代基类请求来源；基类版本读取 `task.payload["url"]`，worker 每领取一个 `Task` 调用一次。不需要 HTTP 时可以继续 `yield Task` 或直接 `yield Item`。
* `start_requests(self)`：零参数重写作为一次性初始请求来源，在 worker 启动前由父进程直接灌入请求队列；只能 yield `Request`。
* `parse(response)`：请求完成后的默认回调。

旧版 `handle_task` 已移除，任务处理统一走 `start_requests(task, ctx)`。

基类 `start_tasks` + `start_requests` 已经覆盖 `start_urls` 的完整链路：重写 `start_tasks` 时请确保 `start_requests` 能处理你的 `Task.payload`；重写 `start_requests` 时则由你的方法处理基类生成的 `{"url": ...}` payload。零参数 `start_requests` 只用于一次性初始请求，不要与 `start_tasks` 混用。

外部任务源示例：

```python
from pcrawl import AsyncSpider, Item, Request, Task


class ExternalTaskSpider(AsyncSpider):
    def start_tasks(self, ctx=None):
        for row in read_tasks_from_somewhere():
            yield Task(route=row["site"], payload=row)

    async def start_requests(self, task, ctx):
        yield Request(
            url=task.payload["url"],
            callback=self.parse,
            meta={"task_id": task.id},
        )

    async def parse(self, response, ctx):
        yield Item(url=response.url, status=response.status)
        yield response.follow(response.css("a.next::attr(href)").get())
```

### 请求去重、重试与 errback

请求入队时默认按 `METHOD + URL（去掉 fragment）+ body` 指纹去重；`Request(dont_filter=True)` 可绕过。`Settings(request_dedup_ttl=...)` 控制指纹保留时间，`0` 表示整个运行期间都保留。

```python
settings = Settings(
    request_max_retries=3,             # 每个 Request 的最大重试次数
    request_retry_delay=1.0,           # 固定/基础重试间隔（秒）
    request_retry_http_codes=(429, 500, 502, 503, 504),
    request_retry_priority_adjust=-1,  # 重试请求降低优先级
    request_retry_max_delay=60.0,      # Retry-After 的上限
    task_max_retries=2,                # Task 自身重试
)
```

* 网络异常和配置的 HTTP 状态码会触发请求级重试；响应带 `Retry-After` 时优先使用它，并受 `request_retry_max_delay` 限制。
* `Request(max_retries=...)` 可以覆盖单个请求的重试次数。
* 设置 `errback` 后，失败会交给 errback 处理，不再自动重试；errback 收到 `RequestFailure(request, exception, response, retries)`。
* 重试次数耗尽后请求被丢弃并记录 `request_failed`；去重过滤记录 `request_filtered`。

队列默认按入队顺序 FIFO 消费。`Settings(queue_order="priority")` 会按 `priority` 排序，内存、共享内存和 Redis 队列都支持；同优先级内保持入队顺序。`start_tasks` 仍在 worker 启动前一次性灌入任务，流式生产和背压留到后续版本。

## 资源会话

资源类型不内置。`start_resources(resources)` 收到的 `resources` 是 `Purser` 形状的门面：`register_adapter()`、`create_pool()`、`import_resources()` 与 purser 原生签名一致。引擎按执行模式自动选择具体实现：

* `inline` / `async`：进程内 `Purser`
* `thread` / `process`：父进程 `LocalScheduler`，worker 通过 IPC 连接
* 没有创建池：自动关闭资源后端

```python
from pcrawl import AsyncSpider, Item, Settings, Task
from purser import AcquireRequest


class AccountSpider(AsyncSpider):
    settings = Settings(concurrency=2, resource_wait_timeout=5)

    async def start_resources(self, resources):
        resources.create_pool("account", "account", config={"max_slots": 1})
        await resources.import_resources(
            "account",
            [
                {"id": "a1", "username": "u1"},
                {"id": "a2", "username": "u2"},
            ],
            import_validate="on_import",
        )

    def start_tasks(self):
        yield Task(payload={"query": "x"})

    async def open_session(self, session):
        account = await session.acquire(
            "account",
            AcquireRequest(use_limit=10),
        )
        session.user["account"] = account.payload

    async def start_requests(self, task, ctx):
        # browser = await create_browser(ctx.user["account"])  # 用户代码
        yield Item(query=task.payload["query"], account=ctx.user["account"]["username"])

    def close_session(self, session, reason):
        session.user.clear()
```

`session.acquire(pool, lease=None, **overrides)` 的第二个参数接受 `purser.AcquireRequest`、等价 mapping，或用关键字覆盖：

```python
await session.acquire("account", AcquireRequest(tags=["cn"], use_limit=10))
await session.acquire("account", {"tags": ["cn"], "use_limit": 10})
await session.acquire("account", use_limit=10)
```

`consumer` 由 pcrawl 按 `worker_id:session_id` 注入。请求级租约写在 `Request(resources={...})` 中，与 session 级使用同一个 `AcquireRequest` 形状。

后端参数不再写在资源声明里，改用 `Settings(purser_storage=..., purser_coordination=..., purser_sqlite_path=..., purser_redis_url=...)`。`start_resources` 中的 `import_resources()` 会自动启动 embedded `Purser`，可以直接 `await` 数据库或 API 读取再导入。

`start_resources` 只负责 provisioning，不创建 live 浏览器、账号连接或代理客户端；这些对象应在 `open_session` 取得租约后再创建。

### 自定义 Resource

`import_resources()` 既接受普通 dict，也接受 `purser.core.resource.Resource` 子类实例。自定义类型先注册 adapter，再创建同 `kind` 的池：

```python
from dataclasses import dataclass
from typing import Annotated

from purser.adapters.base import ResourceAdapter
from purser.core.resource import PayloadField, Resource
from pcrawl import AsyncSpider


@dataclass
class DeviceResource(Resource):
    kind: str = "device"
    serial: Annotated[str | None, PayloadField(required=True)] = None
    region: Annotated[str | None, PayloadField()] = None


class DeviceAdapter(ResourceAdapter):
    kind = "device"
    resource_class = DeviceResource


class DeviceSpider(AsyncSpider):
    async def start_resources(self, resources):
        resources.register_adapter(DeviceAdapter())
        resources.create_pool("device", "device", config={"max_slots": 1})
        await resources.import_resources(
            "device",
            [
                {"id": "device-2", "serial": "SN-2", "region": "us"},
                DeviceResource(
                    id="device-1",
                    kind="device",
                    serial="SN-1",
                    region="cn",
                ),
            ],
            import_validate="on_import",
        )

    async def open_session(self, session):
        device = await session.acquire("device")
        # 四种模式统一从 payload 读取业务字段
        session.user["device"] = device.payload
```

`LeaseHandle.resource` 在四种模式下都会恢复为 adapter 声明的资源类：`embedded` 直接复用原对象，`thread` / `process` 通过 adapter 的 `resource_class` 注册类型；进程模式要求资源类可从 `module:qualname` 导入，否则会退回通用 `Resource` 并以 `LeaseHandle.payload` 为准。`import_validate="on_import"` 让导入的资源立即可租；否则资源会保持 `NEW`，worker 会等待。`thread` / `process` 模式下的 adapter 应使用可导入的类或字符串路径，不要使用局部类、闭包或带不可序列化状态的实例。

框架负责：

* 根据 Spider 模式选择 embedded 或 local 资源后端；
* 启动/关闭 worker 前获取和释放 session 资源；
* HTTP 请求前后获取和释放 request 资源；
* 在 session 生命周期内按 `lease_ttl / 3` 自动续租；
* 把 `ResourceOutcome` 回传给 purser（成功、限流、封禁、验证码等）；
* `use_limit` 超限时抛出 `ResourceInvalid(QUOTA_EXHAUSTED)`，当前会话被释放并重建；
* `LeaseHandle.renew()` 续租。

浏览器、页面、登录状态、代理客户端都不在框架内。

单进程模式（`inline` / `async` / `thread`）共用一个 `Spider` 实例；`process`
模式下每个子进程创建自己的实例。需要跨 worker 隔离的状态请放在
`session.user`，不要放在 Spider 属性里。

## 执行模式

| Spider 类 | mode | 并发单位 | 资源后端 |
| --- | --- | --- | --- |
| `Spider` | `inline` | 单事件循环、单任务顺序执行 | 自动 `embedded` |
| `AsyncSpider` | `async` | 单事件循环、`concurrency` 个 runtime 协程 | 自动 `embedded` |
| `MultiThreadSpider` | `thread` | `workers` 个 OS 线程，每线程独立事件循环 | 自动 `local` |
| `MultiProcessSpider` | `process` | `workers` 个长驻子进程 + supervisor | 自动 `local` |

`purser_mode` 默认是 `auto`。显式设置 `purser_mode="embedded"` 时，`thread` / `process` 会直接报错；普通项目不需要设置这个字段。

```python
from pcrawl import MultiProcessSpider, Settings


class MyProcessSpider(MultiProcessSpider):
    settings = Settings(workers=4)

    async def start_resources(self, resources):
        resources.create_pool("account", "account", config={"max_slots": 1})
```

进程模式默认用 `multiprocessing.Manager` 同时共享任务队列和请求队列，适合本地示例和测试，但没有崩溃恢复。生产环境请使用 Redis（任务与请求队列会同时切换）：

```python
from pcrawl import MultiProcessSpider, Settings


class MyRedisProcessSpider(MultiProcessSpider):
    settings = Settings(
        workers=8,
        queue_backend="redis",
        redis_url="redis://localhost:6379/0",
    )
```

进程 worker 是长驻进程，由 supervisor 统一启动、等待和超时终止；当前版本不会自动重启崩溃 worker。

下载器默认 `trust_env=False`，不会读取系统 `HTTP_PROXY` / `NO_PROXY`。需要环境代理时显式设置 `Settings(trust_env=True)`。

## 组件与钩子

* `open_spider()` / `close_spider(reason)`：整个运行前后各一次，父进程执行；`reason` 为 `finished`、`error` 或 `CloseSpider` 携带的自定义值。
* `start_urls` / `start_tasks(ctx)`：基类 `start_tasks` 把每个 URL 转成 `Task`；重写后走自定义任务来源，只 yield `Task`。
* `start_requests(task, ctx)`：基类版本把 `task.payload["url"]` 转成 `Request`；重写后可构造任意 `Request` / `Item` / 后续 `Task`。
* `start_requests(self)`：零参数重写，一次性产出初始 `Request`。
* `parse(response[, ctx])`：默认 HTTP 解析入口。
* `open_session(session)` / `close_session(session, reason)`：用户资源对象的创建和销毁。
* `session_routes(session)`：该会话可以消费哪些 route。
* `bind_resource(request, name, handle)`：把 request 级租约暴露给下载器中间件。
* `start_resources(resources[, ctx])`：用 purser 原生 API 注册 adapter、创建池、导入记录；四种执行模式共用。
* `on_task_error(task, exc, ctx)`：自定义任务失败处理。

中间件和 Pipeline 的钩子同时兼容 Scrapy 写法和可选的 `ctx`：

```python
class MyMiddleware:
    def process_request(self, request, spider):            # Scrapy 风格
        ...

class AnotherMiddleware:
    def process_response(self, request, response, spider, ctx):  # 可拿 ctx
        return response
```

| 组件 | 钩子 |
| --- | --- |
| Downloader Middleware | `process_request(request, spider)`, `process_response(request, response, spider)`, `process_exception(request, exc, spider)`, `open()`, `close()` |
| Spider Middleware | `process_task(task, spider)`, `process_item(item, spider)` |
| Pipeline | `process_item(item, spider)`, `open_spider(spider)`, `close_spider(spider)` |

所有钩子都可以额外声明 `ctx` 参数（命名或末位位置参数），引擎会按签名注入。同步和 `async def` 实现都支持。


中间件和 Pipeline：

```python
settings = Settings(
    downloader_middlewares={
        "myproject.middleware:RetryMiddleware": 100,
        "myproject.middleware:ProxyMiddleware": 500,
    },
    spider_middlewares={
        "myproject.middleware:DedupMiddleware": 200,
    },
    item_pipelines={
        "myproject.pipelines:CleanPipeline": 100,
        "myproject.pipelines:StorePipeline": 300,
    },
)
```

组件也接受原来的序列写法，序列中的顺序就是执行顺序。字典写法与 Scrapy 一致：数字越小越先执行；`process_response`、`close` 等反向钩子按相反顺序执行。进程模式下建议使用字符串路径；实例和闭包不保证可跨进程序列化。

## 队列

任务与请求各自使用独立队列；`Settings(queue_backend="redis")` 会同时把两者切到 Redis。

| 实现 | 适用模式 | 特性 |
| --- | --- | --- |
| `MemoryTaskQueue` / `MemoryRequestQueue` | 单进程 | 线程安全、FIFO/priority；请求队列带去重和延迟重试 |
| `SharedMemoryTaskQueue` / `SharedMemoryRequestQueue` | 进程模式 | `Manager` 共享列表和锁，无可见性超时 |
| `RedisTaskQueue` / `RedisRequestQueue` | 进程/分布式 | 按 route 分 zset、`processing` 可见性超时、延迟重试；请求队列额外持久化去重指纹 |

Redis 键布局：

```text
<pcrawl key>:tasks:{routes,queue:<route>,data,processing,deadline,delayed,delayed:data,seq}
<pcrawl key>:requests:{routes,queue:<route>,data,processing,deadline,delayed,delayed:data,dedup,seq}
```

`queue_order="priority"` 在三种队列上都生效；同优先级内保持入队顺序。请求队列的可见性超时用于 worker 崩溃后的回收，生产环境建议使用 Redis。

## 文档

开发文档位于 `docs/`，使用 MkDocs Material：

```bash
pip install -e ".[docs]"
mkdocs serve -a 127.0.0.1:8001
# 严格构建
mkdocs build --strict
```

## 目录

```
pcrawl/
  pcrawl/
    engine.py        # 四种模式、LocalScheduler、supervisor
    runtime.py       # worker 主循环、session、请求处理
    spider.py        # Spider 内核、执行模式预设、SpiderContext
    settings.py      # 配置分层、Scrapy 风格访问器、项目 settings 模块
    resources.py     # ResourceRuntime/PoolHandle、WorkerSession、LeaseHandle、后端
    queue.py         # 任务/请求队列：memory / shared / redis
    http.py          # Request、Response、RequestFailure
    task.py          # Task
    utils.py         # 回调签名适配、去重指纹、可序列化回调
    downloader.py    # httpx
    middleware.py    # downloader / spider middleware
    pipeline.py
    signals.py
    stats.py
    cli.py
  docs/              # MkDocs 文档站
  examples/
  tests/
```

## 已知边界

* 资源对象不内置；`account`/`proxy`/浏览器都是 Spider 用户代码。
* `purser_mode` 默认 `auto`；线程/进程自动使用同进程 `LocalScheduler`，跨主机调度仍不在 MVP 内。
* 只映射了一部分 Scrapy 配置名；`CONCURRENT_REQUESTS_PER_DOMAIN`、`DOWNLOAD_DELAY`、`AUTOTHROTTLE_*` 等未实现，写入后只会作为扩展键保存，不会改变运行时。
* 进程模式下信号处理器和 Spider 实例在子进程内独立；Spider 级 `open_spider` / `close_spider` 只在父进程执行一次，主进程只汇总统计。
* 请求去重只对入队请求生效；已经出队、正在处理的请求不会因重复入队被撤回。默认指纹不包含 headers，需要按会话/代理区分时请设置 `dont_filter=True` 并自行去重。
* `start_tasks` 只在 worker 启动前执行一次，暂不支持运行中持续生产和背压。
* Redis 请求队列的 `Retry-After` 只解析秒数形式，不支持 HTTP-date。
* 分布式跨主机调度、purser serve、worker 自动重启不在当前 MVP 内。
