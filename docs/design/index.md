# 设计概览

这一部分解释 pcrawl 为什么这样拆分，以及哪些边界是刻意保留的。

## 阅读顺序

1. [整体架构](architecture.md)：引擎、worker、队列、会话如何协作；
2. [资源后端](resource-backends.md)：embedded / local / noop 三种后端的差异与类型恢复；
3. 指南中的 [执行模式](../guides/execution-modes.md) 与 [资源会话](../guides/resources.md)：使用层面的约束。

## 设计目标

| 目标 | 体现 |
| --- | --- |
| Scrapy 形状、低学习成本 | `start_requests` / `parse` / `Request` / `Response` / 中间件 / Pipeline |
| 任务与请求分离 | `Task` 只存业务字段，HTTP 工作全部在 `Request` |
| 资源与采集解耦 | pcrawl 托管 purser 租约，不定义账号、浏览器、代理对象 |
| 模式可切换 | 同一份 Spider 声明可在 inline/async/thread/process 运行 |
| 无资源也能用 | 没有池时自动关闭资源后端，不依赖 purser |
| 可运维 | 双队列、Redis visibility timeout、统计与信号、显式关闭流程 |

## 刻意不做

- **不内置浏览器/账号/代理实现**：这些属于 Spider 或独立库；
- **不自动应用代理字段**：默认下载器只处理 URL、方法、头、body 和超时；
- **不重启崩溃 worker**：进程 supervisor 只负责等待与超时终止，重启交给外部编排；
- **不做分布式追踪**：信号与统计是进程内原语，跨进程汇总有限；
- **不提供结果持久化**：Item Pipeline 由用户实现；
- **不做动态扩缩容**：worker 数在启动时确定。

## 核心不变量

1. `execution_mode` 只由 Spider 类决定，配置不能覆盖；
2. 资源声明必须可序列化，不能携带 live 对象；
3. 没有池时 `purser_mode=auto` 解析为 `off`；
4. 声明了资源却没有池，启动失败而不是静默降级；
5. 会话级租约在 `open_session` 之前获取，在 `close_session` 之后释放；
6. `process` 模式的 Spider、资源类、回调必须可导入；
7. 队列只做存储，重试与优先级策略在 runtime。

## 取舍

| 取舍 | 原因 | 代价 |
| --- | --- | --- |
| 双队列轮转 | 防止任务生成阶段饿死请求 | 调度逻辑比单队列复杂 |
| 每 worker 一份组件实例 | 避免进程/线程间共享可变状态 | 组件初始化成本按 worker 放大 |
| 泛型 `LeaseHandle` | 四种模式统一 API | 类型信息在跨进程时可能降级 |
| 默认 `spawn` | Windows 兼容、状态干净 | 启动慢，要求可导入 |
| 资源后端 `off` 路径 | 普通 HTTP 采集零依赖 | 显式 `off` + 资源声明会在运行期失败 |

## 扩展点

| 需求 | 扩展位置 |
| --- | --- |
| 新队列后端 | 实现 `TaskQueue` / `RequestQueue` |
| 新资源后端 | 实现 `ResourceBackend` 并接入 `make_backend` |
| 新资源类型 | `Resource` 子类 + `ResourceAdapter` |
| 请求改造 | 下载器中间件 |
| 任务/Item 改造 | Spider 中间件 / Pipeline |
| 生命周期观测 | 信号、`Stats`、日志 |
