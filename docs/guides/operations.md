# 关闭、重试与运维

这一页讲运行期行为：如何启动、如何优雅停止、哪些失败会被重试，以及进程/队列层面的运维边界。

## 启动方式

### CLI

```bash
pcrawl run myproject.spiders.MySpider \
  --settings-module myproject.settings \
  --workers 4 \
  --concurrency 16 \
  --queue redis \
  --redis-url redis://redis:6379/0 \
  --purser auto \
  -s DOWNLOAD_TIMEOUT=15 \
  --json
```

也可以使用模块入口：

```bash
python -m pcrawl run myproject.spiders.MySpider
```

### Python

```python
from pcrawl import CrawlerEngine

if __name__ == "__main__":
    engine = CrawlerEngine(
        MySpider,
        settings_module="myproject.settings",
        settings={"workers": 4},
    )
    stats = engine.run()
    print(stats)
```

!!! warning "Windows / spawn 必须加主模块保护"
    `process` 模式和 `purser_start_method="spawn"` 会重新导入主模块。启动逻辑必须放在
    `if __name__ == "__main__":` 下，Spider 类必须定义在可导入的模块顶层。
    局部类、闭包中的类无法被子进程重建。

## 优雅关闭

在任意回调、`errback` 或资源钩子里抛 `CloseSpider(reason)`：

```python
from pcrawl import CloseSpider


def parse(self, response, ctx):
    if response.status == 401:
        raise CloseSpider("auth_expired")
    yield Item(url=response.url)
```

关闭流程：

1. 当前 runtime 记录 `close_reason`，停止领取新工作；
2. `close_session(session, reason)` 被调用，会话租约释放；
3. `thread` / `async` 的所有 runtime 退出；
4. `process` 模式设置共享 stop event，supervisor 等待子进程；
5. 超过 `shutdown_timeout` 仍未退出的子进程被 `terminate()`；
6. 父进程调用一次 `close_spider(reason)`，关闭队列与资源，发送 `SPIDER_CLOSED`。

`CloseSpider` 不会抛到调用方；`engine.run()` 会正常返回统计。其他未捕获异常会以 `close_reason="error"` 清理后重新抛出。

在进程模式下按 `Ctrl+C`，supervisor 捕获 `KeyboardInterrupt`、设置 stop event，再走同一套退出流程。

## 两种 keep-alive

| 设置 | 作用 |
| --- | --- |
| `keep_alive=False`（默认） | 队列为空时 worker 退出 |
| `keep_alive=True` | worker 常驻，持续等待新任务，直到收到关闭信号 |
| `idle_timeout` | 已开会话中持续没有可领取条目（空队列或无匹配路由）时，等待多久后结束本次会话 |

`idle_timeout` 不是“空闲多久退出进程”。真正决定空队列行为的是 `keep_alive`；常驻 worker 需要配合外部停止信号或 `CloseSpider`。

## 重试语义

### 请求

| 情况 | 行为 |
| --- | --- |
| 下载异常（连接失败、超时等） | 无 errback 时按 `request_max_retries` 重试 |
| 状态码命中 `request_retry_http_codes` | 同上，优先使用 `Retry-After` |
| 回调抛普通异常 | ack 后重新抛出，当前 worker 退出 |
| 回调抛 `IgnoreRequest` | ack，计 `request_ignored` |
| 配置了 `errback` | 失败交给 errback，不自动重试 |
| `dont_filter=False` 但命中指纹 | 不入队，计 `request_filtered` |

请求重试会修改 `Request.retries` 并按 `request_retry_priority_adjust` 调整优先级。需要幂等：同一请求可能被重复执行，尤其是 Redis 的 visibility timeout 到期重投。

### 任务

| 情况 | 行为 |
| --- | --- |
| `start_requests` 抛异常 | `on_task_error` → `TASK_FAILED` → 重试或丢弃 |
| 抛 `IgnoreRequest` | ack，计 `task_ignored` |
| 超过 `task_max_retries` | ack，计 `task_dropped` |

`on_task_error(task, exc, ctx)` 适合记录失败任务；它自身抛异常只记日志，不影响重试判定。

## 队列运维

| 关注点 | 建议 |
| --- | --- |
| 崩溃恢复 | 生产使用 `queue_backend="redis"`；内存/共享内存队列不恢复在途条目 |
| 可见性超时 | `visibility_timeout` 要大于单条任务/请求的最长处理时间，否则会被重复投递 |
| 去重 | 默认永久去重；需要重跑同一批 URL 时提高 `request_dedup_ttl` 或使用 `dont_filter=True` |
| 隔离环境 | 不同项目使用不同的 `redis_key` 前缀 |
| 延迟重试 | Redis 用 delayed zset；`task_retry_delay` 很长时不要用内存队列阻塞 worker |
| 队列积压 | `size()` 可读，但没有内置背压；`start_tasks` 不要无限生成，按批次投递 |

Redis 的 key 结构见 [队列与持久化](queues.md)。

## 资源运维

- `resource_wait_timeout <= 0` 表示无限等待会话资源；生产环境建议设置有限值，避免 worker 永久卡住。
- 会话资源不可用时抛 `ResourceInvalid(outcome)`，租约按该 outcome 归还并重建会话。
- 心跳按 `max(0.1, lease_ttl / 3)` 续租；`lease_ttl` 过短会增加 IPC 压力，过长会延迟故障资源的回收。
- `process` 模式的资源类必须可从 `module:qualname` 导入；否则 worker 退回通用 `Resource`，此时只保证 `payload` 可用。
- 显式 `purser_mode="embedded"` 不能用于 `thread` / `process`，启动时会报 `ConfigError`。

## 进程 supervisor

`process` 模式的父进程只做四件事：启动子进程、等待、汇总统计、超时终止。它**不会重启崩溃的 worker**。生产环境请交给外部编排：

| 场景 | 方案 |
| --- | --- |
| 单机常驻 | systemd / supervisor，进程退出即拉起 |
| 容器 | Kubernetes Deployment / Docker restart policy |
| 横向扩展 | 多个副本共享 Redis 队列和 Redis 协调层 |
| 优雅下线 | 发送 `SIGTERM`，supervisor 会先等待 `shutdown_timeout` |

注意：父进程收到 `SIGTERM` 时，`KeyboardInterrupt` 分支之外的行为取决于运行环境；容器场景建议先在应用层调用 `engine.run()` 并处理信号，或依赖编排系统的强制终止。

## 故障排查顺序

1. 看最终 stats：`task_dropped` / `request_failed` 是否集中出现；
2. 看 `pcrawl.runtime` 日志的重试记录和最终丢弃原因；
3. 看 `pcrawl.resources` 是否有心跳失败、租约获取超时；
4. 检查队列积压与 `visibility_timeout` 是否过短；
5. 检查 Redis 前缀是否与其他环境冲突；
6. 进程模式确认 Spider 与资源类可导入、主模块有 `__main__` 保护。
