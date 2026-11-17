# Python API

`pcrawl` 的公开导出以 `pcrawl/__init__.py` 为准。本页给出常用签名与语义；字段默认值以源码为准。

## 引擎

### `CrawlerEngine`

```python
CrawlerEngine(
    spider: type[Spider] | Spider,
    settings: Settings | Mapping[str, Any] | None = None,
    *,
    settings_module: str | ModuleType | None = None,
    task_queue: TaskQueue | None = None,
    request_queue: RequestQueue | None = None,
    signals: SignalManager | None = None,
    stats: Stats | None = None,
)
```

| 属性/方法 | 说明 |
| --- | --- |
| `mode` | 由 Spider 类解析出的 `inline` / `async` / `thread` / `process` |
| `settings` | 合并后的 `Settings` |
| `signals` / `stats` | 引擎级信号与统计 |
| `resources` | 启动后的 `ResourceManager`；未启动为 `None` |
| `scheduler` | `local` 资源后端时的 `LocalScheduler` |
| `run()` | 同步执行，返回 `stats.snapshot()` |
| `run_async()` | 在已有事件循环中执行 |

构造时会完成设置合并、模式解析和 `validate()`；`thread` / `process` 显式使用 `purser_mode="embedded"` 会立即报 `ConfigError`。

## Spider

| 类 | 说明 |
| --- | --- |
| `Spider` | `inline` 基类 |
| `AsyncSpider` | `execution_mode="async"` |
| `MultiThreadSpider` | `execution_mode="thread"` |
| `MultiProcessSpider` | `execution_mode="process"` |

钩子详见 [Spider 钩子](spider.md)。

### `ResourceContext`

```python
ResourceContext(spider, settings, mode)
```

### `SpiderContext`

```python
SpiderContext(
    spider, settings, session, stats, signals,
    worker_id, runtime, downloader,
)
```

`get_lease(name)` 与 `user` 是 `session.get(name)` / `session.user` 的快捷方式。

## 数据对象

### `Task`

```python
Task(
    id: str = uuid4().hex,
    route: str = "",
    payload: dict[str, Any] = {},
    priority: int = 0,
    retries: int = 0,
    max_retries: int | None = None,
    created_at: float = time.time(),
)
```

`to_dict()` / `from_dict()` 用于队列序列化。

### `Request`

```python
Request(
    url: str,
    method: str = "GET",
    headers: dict[str, str] = {},
    body: bytes | str | None = None,
    callback: str | Callable | None = None,
    errback: str | Callable | None = None,
    meta: dict[str, Any] = {},
    cb_kwargs: dict[str, Any] = {},
    priority: int = 0,
    dont_filter: bool = False,
    route: str = "",
    resource_specs: dict[str, Any] = {},
    retries: int = 0,
    max_retries: int | None = None,
    parent_task_id: str | None = None,
    id: str = uuid4().hex,
)
```

| 方法 | 说明 |
| --- | --- |
| `copy(**changes)` | 复制并覆盖字段 |
| `to_dict()` / `from_dict()` | 队列序列化；bytes body 会 base64 编码 |

跨进程/Redis 时 `callback` 与 `errback` 只能是字符串、模块级函数或绑定到 spider 的方法。

### `Response`

| 属性/方法 | 说明 |
| --- | --- |
| `url` / `status` / `headers` / `body` / `request` / `meta` | 原始响应 |
| `text` | 按 `encoding`（默认 UTF-8）解码，错误替换 |
| `json()` | 解析 `text` |
| `ok` | `200 <= status < 400` |
| `selector` | parsel `Selector`，惰性创建 |
| `base_url` | 考虑 `<base href>` 的基准地址 |
| `css(query)` / `xpath(query)` | 选择器快捷方式 |
| `urljoin(url)` | 基于 `base_url` 拼接 |
| `follow(url, callback=None, **kwargs)` | 创建新 `Request`，默认继承当前 callback/meta/cb_kwargs/route/priority |
| `follow_all(urls, callback=None, **kwargs)` | 生成多个 `Request` |

### `RequestFailure`

errback 收到的对象：

```python
RequestFailure(request, exception=None, response=None, retries=0)
```

### `Item`

`dict` 子类；普通 `Mapping` 会在进入 Pipeline 前转为 `Item`。

## 配置

### `Settings`

```python
Settings(**values)
Settings.load(settings_module=None, *, overrides=None, base=None,
              use_env=True, config_file="pcrawl.cfg")
Settings.from_dict(data)
Settings.from_module(module, *, base=None)
```

| 方法 | 说明 |
| --- | --- |
| `get` / `getint` / `getfloat` / `getbool` / `getlist` / `getdict` | 读取与转换 |
| `set` / `update` / `setdefault` / `setdict` / `setmodule` | 写入并标记显式值 |
| `explicit_values()` / `to_dict()` | 仅显式覆盖 / 全量 |
| `copy()` / `replace(**changes)` | 复制 |
| `validate()` | 枚举与数值校验 |

字段与别名见 [配置项](configuration.md)。

## 资源

### `ResourceSpec`

```python
ResourceSpec(
    pool: str,
    scope: Literal["session", "request"] = "session",
    tags: frozenset[str] = frozenset(),
    slots: int = 1,
    use_limit: int | None = None,
    timeout: float | None = None,
    required: bool = True,
    required_tag: str | None = None,
)
```

- `scope` 决定租约在会话期还是请求期持有；
- `slots` 是单次租约占用的槽位数；
- `use_limit` 超限抛 `ResourceInvalid(QUOTA_EXHAUSTED)`；
- `required_tag` 传给后端按标签筛选；
- `required` 字段当前保留，不影响调度行为。

`from_value()` 接受 `ResourceSpec`、池名字符串或映射。

### `PoolSpec`

```python
PoolSpec(
    kind: str,
    config: Mapping[str, Any] = {},
    seed: tuple[Any, ...] | Mapping | None = (),
    import_options: Mapping[str, Any] = {},
)
```

`config` 传给 `Purser.create_pool`；`seed` 在池创建后导入；`import_options` 传给 `import_resources`。保持可序列化。

### `ResourceSetup`

```python
ResourceSetup(
    pools: Mapping[str, PoolSpec] = {},
    storage: str | None = None,
    coordination: str | None = None,
    sqlite_path: str | None = None,
    redis_url: str | None = None,
    adapters: tuple[Any, ...] = (),
)
```

`from_value()` 接受 `ResourceSetup` / 映射 / `None`；拒绝 live 对象。

### `ResourcePlan`

`ResourcePlan(specs={...})`，提供 `session_specs()` / `request_specs()` 分组。通常无需直接构造，`Spider.resources` 会自动转换。

### `LeaseHandle`

| 属性/方法 | 说明 |
| --- | --- |
| `name` / `pool` / `token` / `resource_id` | 租约标识 |
| `payload` | 资源 payload 的副本 |
| `resource` | 后端恢复的资源对象 |
| `tags` / `used` / `outcome` / `released` | 状态 |
| `mark_used(count=1)` | 累加使用次数，超 `use_limit` 抛 `ResourceInvalid` |
| `set_outcome(outcome)` | 设置归还 outcome |
| `renew()` | 手动续租，返回是否成功 |
| `release(outcome=None, used=None)` | 归还租约，幂等 |

### `WorkerSession`

| 方法 | 说明 |
| --- | --- |
| `get(name)` / `session[name]` / `name in session` | 访问租约 |
| `acquire(name, spec=None, **overrides)` | 获取额外租约 |
| `release(name, outcome=None, used=None)` | 释放单个租约 |
| `release_all(outcome=None)` | 停止心跳并按序释放全部租约 |
| `invalidate(outcome)` | 标记会话全部租约为该 outcome |
| `start_heartbeat(interval)` / `stop_heartbeat()` | 手动控制心跳 |

### `ResourceOutcome`

| 值 | 含义 |
| --- | --- |
| `OK` | 正常使用 |
| `CAPTCHA` | 遇到验证码 |
| `RATE_LIMITED` | 被限流 |
| `BANNED` | 被封禁 |
| `TIMEOUT` | 资源侧超时 |
| `NETWORK_ERROR` | 网络失败（默认失败 outcome） |
| `QUOTA_EXHAUSTED` | 使用次数耗尽 |
| `PREEMPTED` | 被抢占 |

请求级租约覆盖下载阶段（含下载器中间件）；下载成功默认 `OK`，下载异常默认 `NETWORK_ERROR`，回调阶段不在租约作用域内。可在下载器中间件写 `request.meta["pcrawl_resource_outcome"]`，支持枚举值或字符串。

## 队列

| 类 | 用途 |
| --- | --- |
| `TaskQueue` / `RequestQueue` | 抽象接口：`open` / `close` / `put` / `claim` / `ack` / `nack` / `size` |
| `MemoryTaskQueue(poll_interval=0.05, order="fifo")` | 进程内任务队列 |
| `MemoryRequestQueue(poll_interval=0.05, order="fifo", dedup_ttl=0.0)` | 进程内请求队列 |
| `SharedMemoryTaskQueue(...)` | Manager 共享任务队列 |
| `SharedMemoryRequestQueue(...)` | Manager 共享请求队列 |
| `RedisTaskQueue(settings)` | Redis 任务队列 |
| `RedisRequestQueue(settings)` | Redis 请求队列 |

`claim(routes=None, timeout=0.0)` 返回条目或 `None`；`put` 返回 `False` 表示被去重过滤。

## 观测

### `Stats`

`inc(key, value=1)` / `set(key, value)` / `get(key, default=0)` / `snapshot()` / `merge(values)`。

### `SignalManager`

`connect(signal, handler)` / `disconnect(signal, handler)` / `send(signal, **kwargs)`。信号常量与参数见 [信号与统计](../guides/observability.md)。

## 错误

`PcrawlError` 及其子类、outcome 与重试映射见 [错误处理](errors.md)。
