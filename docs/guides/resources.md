# 资源会话

pcrawl 只托管 purser 租约，不内置任何资源类型。账号、代理、浏览器、设备都由你的 `Spider` 子类声明和使用。

## 不需要资源时

不声明 `resources`、`start_resources` 不返回资源池时，`purser_mode="auto"` 会把后端切到 `off`：

- 不启动 `Purser` / `LocalScheduler`；
- 不连接 purser，也不导入 purser；
- 四种执行模式都按普通 HTTP 采集运行。

!!! warning "不要留下悬空声明"
    在 `purser_mode="auto"` 下，如果 `resources = {...}` 声明了规格，但 `start_resources` 没有返回对应 pool，引擎会抛
    `ConfigError("no resource pools")`，而不是静默关闭。真正不用资源时把 `resources` 一并留空。

## 声明资源

用户侧入口只有 `start_resources`，返回可序列化的 `ResourceSetup`：

```python
from pcrawl import AsyncSpider, Item, PoolSpec, ResourceSetup, ResourceSpec, Task


class AccountSpider(AsyncSpider):
    resources = {"account": ResourceSpec(pool="account", use_limit=10)}

    async def start_resources(self):
        return ResourceSetup(
            pools={
                "account": PoolSpec(
                    kind="account",
                    config={"max_slots": 1},
                    seed=[
                        {"id": "a1", "username": "u1"},
                        {"id": "a2", "username": "u2"},
                    ],
                    import_options={"import_validate": "on_import"},
                )
            }
        )

    def start_tasks(self):
        for index in range(10):
            yield Task(payload={"query": f"item-{index}"})

    def open_session(self, session):
        session.user["account"] = session.get("account").payload

    def start_requests(self, task, ctx):
        ctx.get_lease("account").mark_used()
        yield Item(query=task.payload["query"], account=ctx.user["account"]["username"])
```

| 对象 | 作用 |
| --- | --- |
| `resources` | 逻辑名 → `ResourceSpec`，声明“这个 Spider 要租什么” |
| `ResourceSetup` | 后端参数、池定义、初始记录和 adapter |
| `PoolSpec` | 单个池的 `kind`、`config`、`seed`、`import_options` |
| `ResourceSpec` | 单个逻辑资源的 `pool`、`scope`、`tags`、`slots`、`use_limit`、`timeout`、`required_tag` |

`ResourceSetup` 还支持：

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `storage` | `memory` | purser 存储层：`memory` / `sqlite` / `mysql` |
| `coordination` | `memory` | purser 协调层：`memory` / `redis` |
| `sqlite_path` | `None` | `storage="sqlite"` 时的数据库路径 |
| `redis_url` | `None` | `coordination="redis"` 时的连接地址 |
| `adapters` | `()` | 自定义资源 adapter，启动前注册 |

不要返回 live `Purser` / `ResourcePool` 对象；四种模式共用同一套声明。

## 两种作用域

### session 级

`scope="session"`（默认）的租约在 worker 会话开始时获取，会话结束时释放。一个 worker 会话对应一个 `WorkerSession`：

```python
def open_session(self, session):
    account = session.get("account")       # LeaseHandle
    session.user["client"] = create_client(account.payload)

def close_session(self, session, reason):
    session.user["client"].close()
    session.user.clear()
```

- `session.user` 是唯一的 worker 本地状态容器；进程模式下每个子进程有独立 Spider 实例，状态放这里才会按会话隔离。
- 会话租约由框架按 `max(0.1, lease_ttl / 3)` 自动续租；`LeaseHandle.renew()` 可手动续租。
- `use_limit` 超限抛 `ResourceInvalid(QUOTA_EXHAUSTED)`，当前会话释放并按任务重试策略处理。

### request 级

请求级资源声明在 `Request.resource_specs`，只在该请求下载前后持有：

```python
from pcrawl import Request


class ProxySpider(AsyncSpider):
    def start_requests(self, task, ctx):
        yield Request(
            url=task.payload["url"],
            resource_specs={"proxy": {"pool": "proxy", "scope": "request"}},
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

`PoolSpec.seed` 既接受 dict，也接受 `purser.core.resource.Resource` 子类实例。自定义类型需要注册配套 adapter，且 `PoolSpec.kind` 与 `adapter.kind` 一致：

```python
from dataclasses import dataclass
from typing import Annotated

from pcrawl import AsyncSpider, PoolSpec, ResourceSetup, ResourceSpec
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
    resources = {"device": ResourceSpec(pool="device")}

    async def start_resources(self):
        return ResourceSetup(
            pools={
                "device": PoolSpec(
                    kind="device",
                    config={"max_slots": 1},
                    seed=[DeviceResource(id="d1", serial="SN-1")],
                    import_options={"import_validate": "on_import"},
                )
            },
            adapters=(DeviceAdapter,),
        )

    def open_session(self, session):
        device = session.get("device")
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
| 启动 | 注册 adapter、创建池、导入 `PoolSpec.seed` |
| worker 会话开始 | 获取 session 级租约，调用 `open_session` |
| 请求前 | 获取 `request.resource_specs`，调用 `bind_resource` |
| 请求后 | 按 outcome 释放 request 级租约 |
| 会话结束 | 调用 `close_session`，随后释放 session 级租约 |
| 关闭 | 关闭后端、调度器和队列 |

如果在 `open_session` 中确认资源不可用，抛 `ResourceInvalid(outcome)`，框架会用该 outcome 归还租约并重建会话。

## 后端选择

| 模式 | `purser_mode="auto"` 的结果 |
| --- | --- |
| `inline` / `async` | `embedded`：进程内 `Purser` |
| `thread` / `process` | `local`：父进程 `LocalScheduler`，worker 通过 IPC 连接 |
| 没有资源池 | `off`：不启动后端 |

显式设置 `purser_mode="embedded"` 时，`thread` / `process` 会在构造阶段报错；这是有意的 fail-fast。普通项目保持 `auto` 即可。

## 动态导入与生成

`PoolSpec.seed` 在 worker 启动前导入一次。需要在后端启动后读取数据库或动态生成时，覆写 `setup_resources`：

```python
async def setup_resources(self, resources, ctx):
    await resources.import_resources(
        "account",
        await fetch_accounts_from_api(),
        import_validate="on_import",
    )
```
