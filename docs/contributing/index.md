# 贡献概览

pcrawl 的目标是保持小的核心与清晰的边界。提交改动前，请先确认它属于框架能力，而不是某个具体业务的资源实现。

## 开发环境

```bash
# 建议与 purser 同级存放
pip install -e ../purser
pip install -e .[dev]
```

要求 Python 3.10+。Windows 下建议使用 `py -3.10` 显式指定解释器。

## 常用命令

```bash
py -3.10 -m pytest
py -3.10 -m pytest tests/test_engine.py -q
py -3.10 -m ruff check .
py -3.10 -m mkdocs serve -a 127.0.0.1:8001
```

`pytest` 的 `pythonpath` 已包含 `../purser`，无需额外设置 `PYTHONPATH`。文档依赖通过 `.[docs]` 安装。

## 代码约定

- 目标 Python 3.10，行宽 100；
- 公开接口保持同步/异步都可调用，使用现有 `maybe_await` / `call_*` 辅助函数；
- 新增配置项必须进入 `Settings` 字段并补 `validate()`，Scrapy 别名按需加入 `_ALIASES`；
- 资源声明必须可序列化，不要把 live 对象塞进 `ResourceSetup` / `Task` / `Request`；
- 进程模式新增的类必须可在模块顶层导入（`spawn` 会重新导入）；
- 不改变 `execution_mode` 的来源，也不新增覆盖它的配置。

## 改动范围

| 改动 | 需要同步 |
| --- | --- |
| 新公开类/函数 | `pcrawl/__init__.py`、本参考文档 |
| 新配置项 | `Settings`、校验、配置参考、测试 |
| 新队列后端 | `queue.py`、`CrawlerEngine` 选择逻辑、process 模式兼容 |
| 新资源后端 | `resources.py`、`make_backend`、设计文档 |
| 行为修复 | 回归测试，必要时更新指南 |

## 文档

文档使用 MkDocs Material：

```bash
pip install -e .[docs]
mkdocs serve -a 127.0.0.1:8001
mkdocs build --strict
```

- 内部链接使用相对路径（`../guides/...`）；
- 示例必须能对应现有 API，不要展示不存在的参数；
- `process` / `redis` 相关示例要标注前置条件；
- 行为未实现的字段不要写成已生效。

## 提交前检查

- [ ] `pytest` 通过；
- [ ] `ruff check .` 通过；
- [ ] `mkdocs build --strict` 通过；
- [ ] 新增/修改的行为有测试或明确说明；
- [ ] 没有把业务对象、凭据或环境路径写进核心代码与文档。
