# 错误处理

## 异常层次

```text
PcrawlError
├── ConfigError
├── QueueError
├── ResourceError
│   ├── ResourceUnavailable
│   └── ResourceInvalid
├── IgnoreRequest
└── CloseSpider
```

| 异常 | 何时出现 | 运行时行为 |
| --- | --- | --- |
| `ConfigError` | 设置非法、模式与资源后端冲突、缺少池、默认 `start_requests` 缺 `url` | 启动失败或当前回调失败 |
| `QueueError` | Redis 依赖缺失、队列操作失败 | 向上抛出 |
| `ResourceUnavailable` | 获取会话资源超时或失败 | worker 结束当前会话；重试到 `resource_wait_timeout` 后退出 |
| `ResourceInvalid` | 资源已获取但不可用；或 `use_limit` 超限 | 会话标记该 outcome，租约释放，任务/请求走重试 |
| `IgnoreRequest` | 中间件或回调主动跳过 | ack 当前条目，计入 `request_ignored` / `task_ignored` |
| `CloseSpider` | 主动关闭 | ack 当前条目，按 `reason` 优雅停止 |

### `ResourceInvalid`

```python
class ResourceInvalid(ResourceError):
    outcome: ResourceOutcome
```

`outcome` 决定租约归还方式，例如账号被封用 `BANNED`、验证码用 `CAPTCHA`、额度耗尽用 `QUOTA_EXHAUSTED`：

```python
from pcrawl import CloseSpider, ResourceInvalid, ResourceOutcome


def open_session(self, session):
    if not session.get("account").payload.get("cookie"):
        raise ResourceInvalid(ResourceOutcome.BANNED, "account has no cookie")
```

请求阶段抛出时，当前 request 按普通失败路径处理（无 errback 则重试），同时会话被重建。

### `CloseSpider`

```python
CloseSpider(reason="cancelled")
```

`reason` 会传给 `Spider.close_spider(reason)`。抛出的 task/request 不会重试。

### `IgnoreRequest`

用于“跳过但不视为失败”：

```python
from pcrawl import IgnoreRequest


def parse(self, response, ctx):
    if "login" in response.url:
        raise IgnoreRequest()
    yield Item(url=response.url)
```

## `ResourceOutcome`

| 枚举 | 值 | 典型场景 |
| --- | --- | --- |
| `OK` | `ok` | 正常使用 |
| `CAPTCHA` | `captcha` | 需要打码 |
| `RATE_LIMITED` | `rate_limited` | 触发限流 |
| `BANNED` | `banned` | 账号/资源被封 |
| `TIMEOUT` | `timeout` | 资源侧超时 |
| `NETWORK_ERROR` | `network_error` | 网络失败 |
| `QUOTA_EXHAUSTED` | `quota_exhausted` | 使用次数耗尽 |
| `PREEMPTED` | `preempted` | 租约被抢占 |

请求级资源只覆盖下载阶段：下载成功默认归还 `OK`，下载异常默认归还 `NETWORK_ERROR`；回调阶段不在租约作用域内。可在下载器中间件中提前写入：

```python
request.meta["pcrawl_resource_outcome"] = ResourceOutcome.RATE_LIMITED
```

字符串 `"rate_limited"` 同样接受。

## 重试与 errback

### 请求

| 失败来源 | 有 `errback` | 无 `errback` |
| --- | --- | --- |
| 下载异常 | 调用 errback，ack，计 `request_failed` | 未超限则 `request_retried`，否则 `request_failed` |
| 状态码命中 `request_retry_http_codes` | 同上 | 同上，优先 `Retry-After` |
| 回调异常 | 不在回调内部捕获 | ack 后重新抛出，worker 退出 |
| `IgnoreRequest` | — | ack，计 `request_ignored` |
| `CloseSpider` | — | ack，优雅关闭 |

`errback` 收到 `RequestFailure(request, exception, response, retries)`，可以 yield 新的 `Request` / `Item`。

### 任务

| 失败来源 | 行为 |
| --- | --- |
| `start_requests` 任意异常（`CloseSpider` / `IgnoreRequest` 除外） | `on_task_error` → `TASK_FAILED` → 未超限则重试，否则 `task_dropped` |
| `IgnoreRequest` | ack，`task_ignored` |
| `CloseSpider` | ack，优雅关闭 |

task 的重试次数记录在 `Task.retries`；`max_retries` 覆盖设置。

## 日志

| logger | 典型内容 |
| --- | --- |
| `pcrawl.runtime` | 重试警告、最终丢弃错误、回调异常堆栈 |
| `pcrawl.resources` | 心跳失败、租约释放失败 |
| `pcrawl.signals` | 信号处理器异常 |
| `pcrawl.middleware` / `pcrawl.pipeline` | 组件异常 |
| `pcrawl.engine` | 启动/关闭失败 |

信号处理器、`close_session`、`close_spider` 的异常只记录日志，不改变主流程。`close_spider` 的异常不会覆盖原始运行异常。

## 调试建议

1. 先用 `Spider` / `AsyncSpider` 单线程复现；
2. 打开 `log_level="DEBUG"` 观察队列与资源日志；
3. 用 `errback` 记录 `RequestFailure`，区分下载失败与回调失败；
4. 资源问题优先检查 `ResourceOutcome` 是否设置正确；
5. `process` 模式先确认类和回调可导入，再看子进程日志中的 PID。
