# 中间件与 Pipeline

pcrawl 提供三类可插拔组件，全部通过 `Settings` 声明，每个 worker 启动时实例化一次：

| 组件 | 设置项 | 处理对象 | 执行顺序 |
| --- | --- | --- | --- |
| 下载器中间件 | `downloader_middlewares` | `Request` / `Response` / 下载异常 | 请求正序，响应与异常逆序 |
| Spider 中间件 | `spider_middlewares` | `Task` / `Item` | 正序 |
| Item Pipeline | `item_pipelines` | `Item` | 正序 |

!!! note "每个 worker 一份实例"
    `inline` / `async` / `thread` 的 worker 在同一进程内，但各自持有独立的组件实例；
    `process` 的每个子进程也会重新实例化。组件内部状态不要假设全局唯一。

## 声明方式

三种写法等价，都支持“类路径字符串 / 类 / 实例”：

```python
from pcrawl import AsyncSpider, Settings


class Spider(AsyncSpider):
    name = "components"

    settings = Settings(
        downloader_middlewares={
            "myproject.middlewares.ProxyMiddleware": 100,
            "myproject.middlewares.RetryLogMiddleware": 200,
        },
        spider_middlewares=[
            "myproject.middlewares.RouteMiddleware",
        ],
        item_pipelines={
            "myproject.pipelines.CleanPipeline": 100,
            "myproject.pipelines.JsonlPipeline": 200,
        },
    )
```

也接受序列或 `(组件, 优先级)` 对：

```python
settings = Settings(
    item_pipelines=[
        ("myproject.pipelines.CleanPipeline", 100),
        ("myproject.pipelines.JsonlPipeline", 200),
    ]
)
```

规则：

- **数值小的先执行**；
- 映射按优先级排序，序列中未写优先级的项按出现顺序补 `0, 1, 2...`；
- 字符串通过 `load_object` 导入，格式为 `module:qualname` 或 `module.qualname`；
- 类是工厂：有 `from_settings(settings)` 就调用它，否则调用无参构造；
- 实例原样使用。

## 下载器中间件

```python
from pcrawl import Request, Response


class HeaderMiddleware:
    def __init__(self, settings):
        self.timeout = settings.download_timeout

    async def open(self):
        """worker 启动时调用，可省略。"""

    async def process_request(self, request: Request, spider, ctx):
        resource = request.meta.get("pcrawl_resources", {}).get("account")
        if resource:
            request.headers["X-Account-Id"] = resource["resource_id"]
        return None

    async def process_response(self, request, response: Response, spider, ctx):
        if response.status == 403:
            return request.copy(dont_filter=True)  # 交给队列重新调度
        return response

    async def process_exception(self, request, exc, spider, ctx):
        return None  # 返回 None 表示不处理，异常继续向上抛

    async def close(self):
        """worker 关闭时调用，逆序执行。"""
```

`process_request` 的返回值：

| 返回 | 行为 |
| --- | --- |
| `None` | 继续下一个中间件，全部返回 `None` 后真正发起 HTTP |
| `Response` | 短路下载，直接进入响应链 |
| `Request` | 放弃当前请求，把新请求放回队列 |

`process_response` / `process_exception` 返回 `Response`、`Request` 或 `None`（保持当前值）。异常链里返回 `Request` 会重新入队，返回 `Response` 表示已恢复。

钩子签名支持带或不带 `ctx`，同步/异步均可。

!!! warning "默认下载器不识别代理字段"
    `HttpDownloader` 只读取 `Request.url/method/headers/body` 和 `meta["timeout"]`，
    不会自动应用 `request.meta["proxy"]` 或 `proxies`。代理场景需要在中间件里改写请求头，
    或自行替换/扩展下载器；浏览器自动化请放在 `open_session` 里完成。

## Spider 中间件

```python
class RouteMiddleware:
    def process_task(self, task, spider, ctx):
        task.route = task.payload.get("route", task.route)
        return task

    def process_item(self, item, spider, ctx):
        item["source"] = spider.name
        return item
```

- 返回 `None` 表示保留当前对象；
- `process_task` 在 `start_requests` 之前执行，适合补充路由、清洗 payload；
- `process_item` 在 Pipeline 之前执行，适合补充公共字段。

每个 task 的 `start_requests` 抛出的 `IgnoreRequest` 会把 task 计为 `task_ignored` 并 ack；请求阶段的 `IgnoreRequest` 计为 `request_ignored`。

## Item Pipeline

```python
import json
from pathlib import Path


class CleanPipeline:
    def open_spider(self, spider):
        self.seen = set()

    def process_item(self, item, spider, ctx):
        item["title"] = (item.get("title") or "").strip()
        return item

    def close_spider(self, spider):
        pass


class JsonlPipeline:
    def __init__(self, path: str):
        self.path = Path(path)

    @classmethod
    def from_settings(cls, settings):
        return cls(settings.get("output_path", "items.jsonl"))

    def open_spider(self, spider):
        self.file = self.path.open("a", encoding="utf-8")

    def process_item(self, item, spider, ctx):
        self.file.write(json.dumps(dict(item), ensure_ascii=False) + "\n")
        self.file.flush()
        return item

    def close_spider(self, spider):
        self.file.close()
```

- `process_item` 返回 `Item` 或 `Mapping`（会自动转成 `Item`）；返回 `None` 保留原对象；
- `open_spider` / `close_spider` 每个 worker 各调用一次，`close_spider` 逆序；
- 抛异常会让当前 item 失败并向上抛，没有内置的 drop 语义；
- 写文件需要自行 `flush`，框架不保证进程退出前的缓冲行为。

## 何时用中间件、何时用 Spider 钩子

| 需求 | 推荐位置 |
| --- | --- |
| 注入代理、签名、请求头，或替换请求 | 下载器中间件 `process_request` |
| 统一处理 403/429、重试判定 | 下载器中间件 `process_response` |
| 解析前补充 task 路由、数据 | Spider 中间件 |
| 落库、写文件、去重、格式转换 | Item Pipeline |
| 会话级登录、浏览器上下文 | `open_session` / `close_session` |
| 任务级失败记录 | `Spider.on_task_error` |
