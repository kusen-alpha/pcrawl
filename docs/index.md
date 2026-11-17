# pcrawl

<style>
.md-content .md-typeset h1 { display: none; }
</style>

<div class="pcrawl-hero">
  <img class="pcrawl-hero__logo" src="assets/logo.svg" alt="">
  <p class="pcrawl-hero__title">pcrawl</p>
  <p class="pcrawl-hero__tagline">原生支持 purser 资源租约的 Scrapy 风格采集运行时。<br>同一套 Spider 写法，可选同步、异步、多线程与多进程；<br>账号、代理和浏览器对象仍由你的 Spider 子类掌控。</p>
  <p class="pcrawl-hero__actions">
    <a class="md-button md-button--primary" href="getting-started/install/">开始使用</a>
    <a class="md-button" href="getting-started/quickstart/">第一个 Spider</a>
    <a class="md-button" href="reference/">API 参考</a>
  </p>
  <p class="pcrawl-hero__badges">
    <img src="https://img.shields.io/badge/version-0.1.0-009688" alt="版本 0.1.0">
    <img src="https://img.shields.io/badge/python-3.10%2B-009688" alt="Python 3.10+">
    <img src="https://img.shields.io/badge/license-MIT-009688" alt="MIT License">
  </p>
</div>

## 为什么用 pcrawl

<div class="grid cards" markdown>

-   :material-swap-horizontal:{ .lg .middle } __执行模式由类决定__

    ---

    `Spider` / `AsyncSpider` / `MultiThreadSpider` / `MultiProcessSpider`
    对应 inline、async、thread、process 四种模式，切换只需换基类。

    [:octicons-arrow-right-24: 执行模式](guides/execution-modes.md)

-   :material-layers-outline:{ .lg .middle } __Task + Request 双队列__

    ---

    业务任务和 HTTP 请求分层建模、独立消费；请求积压不会阻塞任务转换。

    [:octicons-arrow-right-24: 任务与请求](guides/tasks-and-requests.md)

-   :material-key-chain-variant:{ .lg .middle } __框架托管资源租约__

    ---

    session 级和 request 级租约自动获取、续租、归还；资源类型完全由用户定义。

    [:octicons-arrow-right-24: 资源会话](guides/resources.md)

-   :material-puzzle-outline:{ .lg .middle } __Scrapy 风格组件__

    ---

    Downloader Middleware、Spider Middleware、Item Pipeline、信号与统计，
    组件同时支持同步和 `async def`。

    [:octicons-arrow-right-24: 中间件与 Pipeline](guides/components.md)

</div>

## 最小示例

```python title="spiders.py"
from pcrawl import AsyncSpider, Item, Response


class SimpleSpider(AsyncSpider):
    name = "simple"
    start_urls = ("https://example.com/",)

    def parse(self, response: Response):
        yield Item(
            url=response.url,
            status=response.status,
            title=response.css("title::text").get("").strip(),
        )
```

```bash
pcrawl run spiders:SimpleSpider --json
```

## 文档地图

### 我是接入方

| 目标 | 入口 |
| --- | --- |
| 第一次跑通 | [安装](getting-started/install.md) → [第一个 Spider](getting-started/quickstart.md) |
| 选择执行模式 | [执行模式](guides/execution-modes.md) |
| 从数据库/API 灌任务 | [任务与请求](guides/tasks-and-requests.md) |
| 接账号、代理或浏览器 | [资源会话](guides/resources.md) |
| 写中间件或落库 | [中间件与 Pipeline](guides/components.md) |

### 我是运维/平台方

| 目标 | 入口 |
| --- | --- |
| 配置项目 settings | [配置分层](guides/configuration.md) |
| 多进程与 Redis 队列 | [队列与持久化](guides/queues.md) |
| 优雅关闭、重试与超时 | [关闭、重试与运维](guides/operations.md) |
| 接监控与日志 | [信号与统计](guides/observability.md) |

### 我是贡献者

| 目标 | 入口 |
| --- | --- |
| 查看签名与字段 | [参考](reference/index.md) |
| 理解引擎与资源后端 | [设计与内部](design/index.md) |
| 运行测试与合入门禁 | [测试与工程规范](contributing/testing.md) |

## 权威层级

源码是最终权威；[参考](reference/index.md)描述公开契约；指南与设计页解释语义，可能滞后于实现。行为冲突时按此顺序采信。

当前版本 `0.1.0`。
