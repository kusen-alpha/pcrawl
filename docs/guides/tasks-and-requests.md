# 任务与请求

pcrawl 把“业务任务”和“HTTP 请求”拆成两条独立队列。这样任务转换与请求下载互不阻塞，也让重试、去重和持久化各自落在合适的层级。

```text
start_urls
   │ 基类 start_tasks：每个 URL -> Task(payload={"url": url})
   ▼
TaskQueue ──claim Task──> start_requests(task, ctx) ──> RequestQueue
                                                          │
                                      claim Request -> 下载 -> callback / errback
```

## Task

`Task` 只承载业务字段，不包含 URL、headers、body 等 HTTP 语义。

```python
from pcrawl import Task

task = Task(
    route="shop-a",              # 可选，用于按 route 过滤会话
    payload={"sku": "A-1"},      # 业务数据
    priority=10,                 # queue_order="priority" 时生效
    max_retries=1,               # 覆盖 settings.task_max_retries
)
```

| 字段 | 说明 |
| --- | --- |
| `id` | 默认 UUID；重新入队时保持不变 |
| `route` | 业务路由；`Spider.session_routes()` 可限制会话只消费某些 route |
| `payload` | 任意可序列化字典 |
| `priority` | 优先级；同优先级内保持入队顺序 |
| `retries` / `max_retries` | 任务级重试计数与上限 |
| `created_at` | 创建时间戳 |

`start_tasks` 只能 yield `Task`。yield `Request` 或 `Item` 会抛 `TypeError`，这是为了强制把 HTTP 语义放进 `start_requests`。

## Request

```python
from pcrawl import Request

request = Request(
    url="https://example.com/list?page=2",
    method="GET",
    headers={"Referer": "https://example.com/"},
    callback=self.parse_list,
    meta={"timeout": 10},
    priority=5,
    route="shop-a",
    resource_specs={"proxy": {"pool": "proxy"}},
)
```

| 字段 | 说明 |
| --- | --- |
| `url` | 完整 URL |
| `method` / `headers` / `body` | HTTP 请求信息；`body` 支持 `str` 或 `bytes` |
| `callback` / `errback` | 可调用对象、方法名字符串或 `module:function` 路径 |
| `meta` | 请求上下文；`timeout` 会覆盖 `download_timeout` |
| `cb_kwargs` | 回调命名参数 |
| `priority` | 队列优先级；重试时按 `request_retry_priority_adjust` 调整 |
| `dont_filter` | 绕过请求去重 |
| `route` | 路由；`follow()` 默认沿用 |
| `resource_specs` | request 级资源声明，见[资源会话](resources.md) |
| `retries` / `max_retries` | 请求级重试计数与上限 |
| `parent_task_id` | 关联的上游 Task；由框架自动填充 |

`Response` 内置 parsel 选择器，并自动识别页面 `<base href>`：

```python
def parse(self, response):
    title = response.css("h1::text").get()
    data = response.json()                    # JSON 响应
    next_url = response.urljoin("/page/2")
    yield response.follow(next_url, callback=self.parse_page)
    yield from response.follow_all(
        response.css("a.detail::attr(href)").getall(),
        callback=self.parse_detail,
    )
```

`follow()` 默认沿用当前请求的 `callback`、`meta`、`cb_kwargs`、`route` 和 `priority`；传入新的 `callback` 可覆盖。

## 生产入口

| 入口 | 作用 | 允许产出 |
| --- | --- | --- |
| `start_urls` | 最简入口；基类 `start_tasks` 生成 `Task(payload={"url": ...})` | URL 字符串 |
| `start_tasks(ctx)` | 自定义任务源；worker 启动前执行一次 | `Task` |
| `start_requests(task, ctx)` | 每个 Task 转成一个或多个 Request | `Request`、`Item`、`Task`、`Mapping` |
| `start_requests(self)` | 零参数一次性入口；worker 启动前由父进程灌入请求队列 | `Request` |
| `parse(response[, ctx])` | 默认回调 | `Request`、`Item`、`Task`、`Mapping` |

零参数 `start_requests` 只用于初始请求，不要与 `start_tasks` 混用。重写 `start_tasks` 时，基类 `start_requests` 仍期望 `task.payload["url"]`；自定义 payload 请同时重写 `start_requests(task, ctx)`。

## 回调签名

回调、errback、中间件和 Pipeline 钩子都支持两种写法：

- Scrapy 风格：`parse(self, response)`、`process_request(request, spider)`；
- 需要上下文：在末尾加 `ctx` 参数，或使用名为 `ctx` 的参数。

同步函数与 `async def` 都支持。跨进程队列中的 callback/errback 会被序列化为 `module:qualname` 或方法名，因此优先使用可导入的模块级函数或 Spider 方法。

## 去重、重试与 errback

请求入队时按 `METHOD + URL（去掉 fragment）+ body` 生成指纹。`Settings(request_dedup_ttl=...)` 控制保留时间，`0` 表示整个运行期间都保留；`Request(dont_filter=True)` 绕过去重。

```python
from pcrawl import Settings

settings = Settings(
    request_max_retries=3,
    request_retry_delay=1.0,
    request_retry_http_codes=(429, 500, 502, 503, 504),
    request_retry_priority_adjust=-1,
    request_retry_max_delay=60.0,
    task_max_retries=2,
)
```

- 网络异常和配置的 HTTP 状态码触发请求级重试；响应带 `Retry-After` 时优先使用，并被 `request_retry_max_delay` 截断。
- `Request(max_retries=...)` 覆盖单个请求的重试次数。
- 设置 `errback` 后，失败交给 errback，不再自动重试；errback 收到 `RequestFailure(request, exception, response, retries)`。
- 重试耗尽后请求被丢弃并记录 `request_failed`；去重过滤记录 `request_filtered`。

## 队列顺序与边界

`Settings(queue_order="priority")` 在内存、共享内存和 Redis 队列上都生效；同优先级内保持入队顺序。

!!! note "当前边界"
    `start_tasks` 只在 worker 启动前执行一次，暂不支持运行中持续生产和背压。需要持续生产时，可以在回调中继续 yield `Task`，由同一队列消费。
