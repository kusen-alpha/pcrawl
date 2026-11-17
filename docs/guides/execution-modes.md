# 执行模式

执行模式由 Spider 类唯一决定，`Settings` 和 CLI 都不能覆盖。

| Spider 类 | mode | 并发单位 | 资源后端（`auto`） |
| --- | --- | --- | --- |
| `Spider` | `inline` | 单个 runtime，单事件循环顺序执行 | `embedded` |
| `AsyncSpider` | `async` | 单事件循环 + `concurrency` 个 runtime 协程 | `embedded` |
| `MultiThreadSpider` | `thread` | `workers` 个 OS 线程，每线程独立事件循环 | `local` |
| `MultiProcessSpider` | `process` | `workers` 个长驻子进程 + supervisor | `local` |

```python
from pcrawl import AsyncSpider, MultiThreadSpider


class MyAsyncSpider(AsyncSpider):
    custom_settings = {"CONCURRENT_REQUESTS": 8}


class MyThreadSpider(MultiThreadSpider):
    custom_settings = {"WORKERS": 4}
```

切换模式只需换基类；`start_resources`、`open_session`、中间件和 Pipeline 写法不变。若用多继承复用逻辑，模式基类必须放在最左侧（如 `class P(MultiProcessSpider, Base)`），否则会继承到 `Base` 的模式。

## inline

- 一个 `WorkerRuntime`，`concurrency` 不生效。
- 同步回调可以直接写；阻塞调用会阻塞整个 worker。
- 适合脚本式采集、调试和最小依赖场景。

## async

- 单个事件循环内启动 `concurrency` 个 `WorkerRuntime` 协程。
- 回调与组件推荐 `async def`；同步函数不会被放进线程池，长时间阻塞会卡住整个事件循环。
- `concurrency` 不是 HTTP 连接池大小，而是并发 runtime 数。

## thread

- 启动 `workers` 个 OS 线程，每个线程运行自己的事件循环。
- 所有线程共享同一个 Spider 实例，因此**可变的 worker 状态必须放 `session.user`**，不要挂在 Spider 属性上。
- 适合需要复用同一内存对象、但不想多进程序列化成本的场景。

## process

- 启动 `workers` 个长驻子进程，父进程只做 supervisor 与统计汇总。
- 每个子进程创建自己的 Spider 实例；`open_spider` / `close_spider` 只在父进程执行一次。
- 需要共享内存或 Redis 队列；默认 `multiprocessing.Manager`，生产建议 Redis。
- supervisor 等待进程结束，超过 `shutdown_timeout` 后终止；**当前版本不会自动重启崩溃 worker**。

```python
from pcrawl import MultiProcessSpider, Settings


class MyProcessSpider(MultiProcessSpider):
    settings = Settings(
        workers=4,
        queue_backend="redis",
        redis_url="redis://localhost:6379/0",
    )
```

## 如何选择

| 场景 | 建议 |
| --- | --- |
| 抓取逻辑简单、想快速验证 | `Spider` / `AsyncSpider` |
| I/O 密集、回调全 async | `AsyncSpider`，调 `concurrency` |
| 有重 CPU/解析、但不便多进程 | `MultiThreadSpider` |
| 需要进程隔离、横向扩展 | `MultiProcessSpider` + Redis |
| 只做普通 HTTP、不用资源 | 任意模式；资源后端自动 `off` |

## 配置不能覆盖模式

`Settings(mode="async")`、`MODE` 环境键或 CLI 都不接受执行模式。写入 `MODE` / `PCRAWL_MODE` 会抛 `ConfigError`，避免“类说一种、配置说另一种”的不一致。
