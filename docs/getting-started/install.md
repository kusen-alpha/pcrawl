# 安装

## 环境

- Python **3.10+**（源码使用 `X | None` 等新语法）。
- 普通 HTTP 采集只依赖 `httpx`、`parsel`。
- 资源调度依赖同工作区的 `purser`；Redis 队列依赖 `redis`。

## 从源码安装

pcrawl 与 purser 通常并排开发：

```bash
# purser 与 pcrawl 同级时
pip install -e ../purser
pip install -e .[dev]
```

只做普通 HTTP 采集时，purser 可以不安装；`purser_mode="auto"` 在没有资源池时不会导入它。

## extras

| extra | 内容 |
| --- | --- |
| `resources` | `purser>=0.2`，启用资源租约 |
| `dev` | `pytest`、`pytest-asyncio`、`fakeredis` |
| `docs` | `mkdocs-material`，构建本文档站 |

```bash
pip install -e ".[resources,dev]"
```

## 验证

```bash
pcrawl --version
python -c "import pcrawl; print(pcrawl.__version__)"
```

预期输出包含 `pcrawl 0.1.0` 与 `0.1.0`。

## 构建文档

```bash
pip install -e ".[docs]"
mkdocs serve -a 127.0.0.1:8001
```

浏览器打开 `http://127.0.0.1:8001`。发布静态站点用 `mkdocs build`，产物在 `site/`。
