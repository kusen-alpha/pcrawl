# 指南

指南按“任务”组织，而不是按模块组织。先理解两条主线，再按需要跳到具体做法。

## 先读这两页

| 页面 | 解决什么问题 |
| --- | --- |
| [任务与请求](tasks-and-requests.md) | Task 和 Request 为什么要分层？数据从哪进、结果从哪出？ |
| [资源会话](resources.md) | 账号/代理/浏览器怎么声明、怎么租、跨进程怎么保持一致？ |

## 按任务查找

| 任务 | 页面 |
| --- | --- |
| 选择同步、异步、多线程或多进程 | [执行模式](execution-modes.md) |
| 从数据库/API 灌任务，控制重试与去重 | [任务与请求](tasks-and-requests.md) |
| 声明资源池、使用 session/request 级租约 | [资源会话](resources.md) |
| 写下载器中间件、Spider 中间件、Pipeline | [中间件与 Pipeline](components.md) |
| 切换 memory / 共享内存 / Redis 队列 | [队列与持久化](queues.md) |
| 项目 settings、环境变量与 CLI 覆盖 | [配置分层](configuration.md) |
| 接信号、看统计、配日志 | [信号与统计](observability.md) |
| 优雅关闭、超时、重试与部署 | [关闭、重试与运维](operations.md) |

## 概念顺序

建议按以下顺序建立心智模型：

1. **执行模式**由 Spider 类决定，决定 worker 的并发单位和资源后端。
2. **Task → Request** 是两段式生产：Task 只承载业务字段，Request 才进入 HTTP 调度。
3. **WorkerSession** 是租约与用户对象的生命周期边界；跨 worker 状态放 `session.user`。
4. **队列**决定持久化与恢复能力；进程模式优先 Redis。

需要字段级契约时查[参考](../reference/index.md)；需要“为什么这样设计”时查[设计与内部](../design/index.md)。
