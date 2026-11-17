# 测试与工程规范

## 测试布局

| 文件 | 覆盖 |
| --- | --- |
| `tests/test_settings.py` | 分层、别名、显式值与校验 |
| `tests/test_engine.py` | 四种模式、双队列、回调、关闭、资源后端切换 |
| `tests/test_custom_resource.py` | 自定义 `Resource` + adapter、类型恢复 |
| `tests/helpers.py` | 可被子进程导入的 Spider 类 |

## 运行

```bash
# 全量
py -3.10 -m pytest

# 单文件 / 单用例
py -3.10 -m pytest tests/test_engine.py -q
py -3.10 -m pytest tests/test_engine.py::test_process_mode_runs_without_resources -q

# 只跑不依赖 purser 的部分
py -3.10 -m pytest tests/test_settings.py tests/test_engine.py -q
```

`pyproject.toml` 已配置：

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
testpaths = ["tests"]
pythonpath = [".", "../purser"]
```

`pytest-asyncio` 为 auto 模式，`async def` 测试不需要额外标记。自定义资源测试使用 `pytest.importorskip("purser...")`，未安装 purser 时跳过而不是失败。

## 编写进程模式测试

`spawn` 要求目标类可导入，因此：

- Spider 类放在模块顶层（通常 `tests/helpers.py`）；
- 不要在测试函数内定义 `MultiProcessSpider` 子类；
- 使用小 `count` 和短 `idle_timeout`，避免测试等待；
- 需要验证子进程行为时，断言父进程可见的 `Stats` 与 `engine` 状态。

```python
# tests/helpers.py
class ProcessItemSpider(MultiProcessSpider):
    count = 12

    def start_tasks(self):
        for index in range(self.count):
            yield Task(payload={"index": index})

    def start_requests(self, task, ctx):
        yield Item(index=task.payload["index"], worker=ctx.worker_id)
```

```python
def test_process_mode_runs_without_resources():
    engine = CrawlerEngine(ProcessItemSpider, _fast_settings(workers=2))
    stats = engine.run()

    assert engine.settings.purser_mode == "off"
    assert engine.scheduler is None
    assert stats["task_received"] == ProcessItemSpider.count
```

## 资源测试

- 简单资源用 purser 内置类型或 dict seed；
- 自定义类型必须同时定义 `Resource` 子类和 `ResourceAdapter`，并让 `kind` 一致；
- 跨进程测试断言 `type(lease.resource).__name__`，而不是对象身份；
- 释放类测试要显式 `release_all()`，避免心跳任务悬挂。

```python
@dataclass
class DeviceResource(Resource):
    kind: str = "device"
    serial: Annotated[str | None, PayloadField(required=True)] = None


class DeviceAdapter(ResourceAdapter):
    kind = "device"
    resource_class = DeviceResource
```

## 网络与 Redis

- 需要真实 HTTP 的测试使用本地 `ThreadingHTTPServer`，不要访问公网；
- Redis 队列测试使用 `fakeredis`（`dev` 依赖已包含）；
- 测试结束关闭队列与引擎，避免后台线程/子进程残留。

## 静态检查

```bash
py -3.10 -m ruff check .
```

Ruff 配置：

```toml
[tool.ruff]
line-length = 100
target-version = "py310"
```

当前没有强制格式化工具；改动保持与周围代码风格一致。

## 文档构建

```bash
pip install -e .[docs]
mkdocs build --strict
```

`--strict` 会把坏链接、缺失 nav 页面当作错误。新增文档后同时更新 `mkdocs.yml` 的 `nav`。

## 回归清单

修改 runtime / engine 时至少验证：

- [ ] 无资源 + `auto` 仍为 `off`；
- [ ] `async` 模式并发与回调正常；
- [ ] `thread` 模式共享 Spider、状态放 `session.user`；
- [ ] `process` 模式能导入 Spider、统计能汇总；
- [ ] 声明资源但无池会报 `ConfigError`；
- [ ] `embedded` + `thread/process` 会报 `ConfigError`；
- [ ] `CloseSpider` 的 reason 能传到 `close_spider`。
