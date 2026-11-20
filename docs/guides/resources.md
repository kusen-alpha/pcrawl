# 资源会话

pcrawl 只托管 purser 租约，不内置任何资源类型。账号、代理、浏览器、设备都由你的 `Spider` 子类通过 purser 原生 API 声明和使用。

资源分成两层：

- **provisioning**：在 `start_resources(resources)` 中注册 adapter、建池、导入记录；
- **leasing**：在 `open_session` 中按会话获取，或写在 `Request.resources` 中按请求获取。

`resources` 参数是与 `Purser` 同形的门面：`register_adapter()`、`create_pool()`、`import_resources()` 的签名与 purser 一致。pcrawl 根据执行模式在背后选择 embedded `Purser` 或父进程 `LocalScheduler`，Spider 代码不需要分支。

## 不需要资源时

不实现 `start_resources`、也不调用 `create_pool` 时，`purser_mode="auto"` 会把后端切到 `off`：

- 不启动 `Purser` / `LocalScheduler`；
- 不连接 purser，也不导入 purser；
- 四种执行模式都按普通 HTTP 采集运行。

不要在 Spider 上再写 `resources = {...}`：该写法已删除，引擎会直接报 `ConfigError`。

## 声明资源

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
        for index in range(10):
            yield Task(payload={"query": f"item-{index}"})

    async def open_session(self, session):
        account = await session.acquire(
            "account",
            AcquireRequest(use_limit=10, timeout=5),
        )
        session.user["account"] = account.payload

    def start_requests(self, task, ctx):
        ctx.get_lease("account").mark_used()
        yield Item(query=task.payload["query"], account=ctx.user["account"]["username"])
```

`create_pool()` 的参数就是 `Purser.create_pool()` 的参数：`name`、`kind`、`config`、`profile` 以及任意池配置覆盖项：

```python
resources.create_pool(
    "proxy",
    "proxy",
    config={"max_slots": 20},
    profile="validated",
    lease_ttl=120,
)
```

`PoolHandle.import_resources()` 同样与 `ResourcePool.import_resources()` 同形，返回 pcrawl 的 `ImportReport`。

| 对象 | 作用 |
| --- | --- |
| `ResourceRuntime` | `start_resources` 收到的门面；负责 adapter、建池、导入 |
| `PoolHandle` | 单个池的句柄；提供 `import_resources()` / `stats()` |
| `ImportReport` | 与 purser 导入报告同形的只读视图 |
| `WorkerSession.acquire()` | 获取 session 级租约 |
| `Request.resources` | 声明 request 级租约 |

后端参数不再写在资源声明里，改用 `Settings`：

| 设置 | 默认 | 说明 |
| --- | --- | --- |
| `purser_mode` | `auto` | `auto` / `off` / `embedded` / `local` |
| `purser_storage` | `memory` | `memory` / `sqlite` / `mysql` |
| `purser_coordination` | `memory` | `memory` / `redis` |
| `purser_sqlite_path` | `None` | `storage="sqlite"` 时的数据库路径 |
| `purser_redis_url` | `None` | `coordination="redis"` 时的连接地址 |
| `purser_start_method` | `spawn` | `LocalScheduler` 的进程启动方式 |

## 两种租约作用域

### session 级

session 租约在 `open_session` 中显式获取，会话结束时由框架释放：

```python
async def open_session(self, session):
    account = await session.acquire("account", AcquireRequest(use_limit=10))
    session.user["client"] = create_client(account.payload)

def close_session(self, session, reason):
    session.user["client"].close()
    session.user.clear()
```

`session.acquire(pool, lease=None, **overrides)` 的第二个参数接受：

- `purser.AcquireRequest`；
- 等价的普通 mapping，例如 `{"tags": ["cn"], "use_limit": 10}`；
- `None` 加关键字覆盖，例如 `await session.acquire("account", use_limit=10)`。

`consumer` 由 pcrawl 按 `worker_id:session_id` 注入，不需要自己填写。重复获取同一个池会返回同一个租约。

- `session.user` 是唯一的 worker 本地状态容器；进程模式下每个子进程有独立 Spider 实例，状态放这里才会按会话隔离。
- 会话租约由框架按 `max(0.1, lease_ttl / 3)` 自动续租；`LeaseHandle.renew()` 可手动续租。
- `use_limit` 超限抛 `ResourceInvalid(QUOTA_EXHAUSTED)`，当前会话释放并按任务重试策略处理。

### request 级

请求级租约写在 `Request.resources`，key 是池名，value 是 `AcquireRequest` 或等价 mapping：

```python
from pcrawl import Request
from purser import AcquireRequest


class ProxySpider(AsyncSpider):
    def start_requests(self, task, ctx):
        yield Request(
            url=task.payload["url"],
            resources={"proxy": AcquireRequest(tags=["residential"])},
        )
```

框架获取租约后调用 `Spider.bind_resource(request, name, handle)`。默认把快照放进 `request.meta["pcrawl_resources"][name]`，下载器中间件可以直接读取；也可以重写它来追加自定义字段。默认 `HttpDownloader` 不会使用代理字段，代理/浏览器请自行在中间件或会话里接入。

请求级资源在下载阶段（含下载器中间件）持有，下载结束后立即归还；回调 `parse` 不在租约作用域内。

- 下载成功时默认 `OK`；
- 下载异常或中间件抛出未被恢复的异常时默认 `NETWORK_ERROR`；
- 状态码重试与回调阶段的资源使用不计入本次请求租约。

需要在下载结束前调整 outcome 时，可写入：

```python
request.meta["pcrawl_resource_outcome"] = "rate_limited"
```

支持的值见 [ResourceOutcome](../reference/api.md#resourceoutcome)。

## 自定义资源类型

`import_resources()` 既接受 dict，也接受 `purser.core.resource.Resource` 子类实例。自定义类型需要注册配套 adapter，且池的 `kind` 与 `adapter.kind` 一致：

```python
from dataclasses import dataclass
from typing import Annotated

from pcrawl import AsyncSpider
from purser.adapters.base import ResourceAdapter
from purser.core.resource import PayloadField, Resource


@dataclass
class DeviceResource(Resource):
    kind: str = "device"
    serial: Annotated[str | None, PayloadField(required=True)] = None


class DeviceAdapter(ResourceAdapter):
    kind = "device"
    resource_class = DeviceResource


class DeviceSpider(AsyncSpider):
    async def start_resources(self, resources):
        resources.register_adapter(DeviceAdapter())
        resources.create_pool("device", "device", config={"max_slots": 1})
        await resources.import_resources(
            "device",
            [DeviceResource(id="d1", serial="SN-1")],
            import_validate="on_import",
        )

    async def open_session(self, session):
        device = await session.acquire("device")
        assert type(device.resource).__name__ == "DeviceResource"
        session.user["serial"] = device.payload["serial"]
```

`LeaseHandle.resource` 在四种模式下都会恢复为 adapter 声明的资源类：

- `inline` / `async`：embedded purser 直接复用原对象；
- `thread` / `process`：框架通过 `adapter.resource_class` 注册类型，按 `module:qualname` 在 worker 内重建。

进程模式要求资源类可从模块路径导入。局部类、闭包或无法导入的类会退回通用 `Resource`，此时以 `LeaseHandle.payload` 为准。adapter 本身也应使用可导入的类或字符串路径。

## 资源的生命周期

| 阶段 | 框架动作 |
| --- | --- |
| 启动 | 调用 `start_resources`；注册 adapter、创建池、导入记录 |
| worker 会话开始 | 构造 `WorkerSession`，调用 `open_session`；Spider 在其中 `acquire` |
| 请求前 | 获取 `request.resources`，调用 `bind_resource` |
| 请求后 | 按 outcome 释放 request 级租约 |
| 会话结束 | 调用 `close_session`，随后释放全部 session 级租约 |
| 关闭 | 关闭后端、调度器和队列 |

如果在 `open_session` 中确认资源不可用，抛 `ResourceInvalid(outcome)`，框架会用该 outcome 归还租约并重建会话。

## 后端选择

| 模式 | `purser_mode="auto"` 的结果 |
| --- | --- |
| `inline` / `async` | `embedded`：进程内 `Purser` |
| `thread` / `process` | `local`：父进程 `LocalScheduler`，worker 通过 IPC 连接 |
| 没有创建池 | `off`：不启动后端 |

显式设置 `purser_mode="embedded"` 时，`thread` / `process` 会在构造阶段报错；这是有意的 fail-fast。普通项目保持 `auto` 即可。

`start_resources` 中的 `import_resources()` 会自动启动 embedded `Purser`，因此导入不需要等引擎显式启动后端。需要在导入前读取数据库或动态生成数据时，直接在 `start_resources` 中 `await` 即可：

```python
async def start_resources(self, resources):
    resources.create_pool("account", "account", config={"max_slots": 1})
    await resources.import_resources(
        "account",
        await fetch_accounts_from_api(),
        import_validate="on_import",
    )
```
