# 上手

这一组页面只回答一个问题：**怎样在 10 分钟内跑完第一次采集。**

## 选择入口

| 你的情况 | 从这里开始 |
| --- | --- |
| 第一次使用，只想抓一个页面 | [安装](install.md) → [第一个 Spider](quickstart.md) |
| 已有 Scrapy 项目，想迁移组件 | 先看[执行模式](../guides/execution-modes.md)，再看[任务与请求](../guides/tasks-and-requests.md) |
| 要接账号、代理或浏览器 | [资源会话](../guides/resources.md) |
| 需要多进程或 Redis 队列 | [队列与持久化](../guides/queues.md) |

## 最短路径

1. [安装](install.md)：Python ≥ 3.10；需要资源调度时安装 purser。
2. [第一个 Spider](quickstart.md)：继承 `AsyncSpider`，运行 `pcrawl run`。
3. 理解两个边界：**执行模式由 Spider 类决定**，**Task 与 Request 分属两条队列**。
4. 需要资源时进入[资源会话](../guides/resources.md)，需要横向扩展时进入[执行模式](../guides/execution-modes.md)。

## 完成之后

- 想迁移 Scrapy 写法：[任务与请求](../guides/tasks-and-requests.md)、[中间件与 Pipeline](../guides/components.md)。
- 想接真实账号/代理：[资源会话](../guides/resources.md)。
- 想上线多进程：[队列与持久化](../guides/queues.md)、[关闭、重试与运维](../guides/operations.md)。
