# 参考概览

本手册按“稳定接口”和“实现细节”分开：

| 页面 | 内容 | 稳定性 |
| --- | --- | --- |
| [Spider 钩子](spider.md) | 子类可覆写的方法、调用时机、模式差异 | 稳定 |
| [Python API](api.md) | 公开导出的类、方法与字段 | 稳定 |
| [CLI](cli.md) | `pcrawl run` 的参数与输出 | 稳定 |
| [配置项](configuration.md) | 全部字段、别名、默认值 | 稳定 |
| [错误处理](errors.md) | 异常层次、outcome、重试映射 | 稳定 |

## 公开入口

```python
from pcrawl import (
    AsyncSpider,
    CloseSpider,
    CrawlerEngine,
    Item,
    MultiProcessSpider,
    MultiThreadSpider,
    Request,
    ResourceOutcome,
    Response,
    Settings,
    Spider,
    Task,
)
```

完整导出清单见 [Python API](api.md)。日常最小组合是：

```python
from pcrawl import AsyncSpider, CrawlerEngine, Item
```

## 权威顺序

当文档与实现不一致时，按以下顺序判断：

1. 源码与测试；
2. 本参考页；
3. 指南中的示例；
4. README 的历史说明。

发现不一致时，以可运行测试为准，并提 issue 修正文档。

## 版本

当前文档对应 pcrawl `0.1.0`。0.x 阶段接口仍可能调整；破坏性变更会写入发布说明。
