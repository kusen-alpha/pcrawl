# CLI

安装后提供 `pcrawl` 命令，等价入口为 `python -m pcrawl`。

```bash
pcrawl --version
pcrawl run module:SpiderClass [options]
```

## `pcrawl run`

| 参数 | 说明 |
| --- | --- |
| `spider` | 位置参数，Spider 导入路径，如 `myproject.spiders.MySpider` 或 `myproject.spiders:MySpider` |
| `--workers N` | 线程/进程数 |
| `--concurrency N` | async 模式 runtime 数 |
| `--queue {memory,redis}` | 队列后端 |
| `--redis-url URL` | Redis 地址 |
| `--purser {auto,off,embedded,local}` | 资源后端 |
| `--keep-alive` | worker 队列空时保持常驻 |
| `--log-level LEVEL` | 日志级别 |
| `--settings-module MODULE` | 项目设置模块 |
| `-s KEY=VALUE` / `--set KEY=VALUE` | 覆盖单个配置，可重复 |
| `--json` | 最终统计以 JSON 输出 |

### 覆盖优先级

CLI 参数与 `-s` 都会进入引擎覆盖层，优先级高于 Spider 设置：

```bash
pcrawl run myproject.spiders.MySpider \
  --settings-module myproject.settings \
  --workers 8 \
  --queue redis \
  --redis-url redis://localhost:6379/1 \
  -s DOWNLOAD_TIMEOUT=20 \
  -s USER_AGENT='mybot/1.0' \
  -s REQUEST_RETRY_HTTP_CODES='(429, 503)' \
  --json
```

`-s` 的值解析规则：

1. 忽略大小写后等于 `true` / `false` → `bool`；
2. 尝试 `ast.literal_eval`，支持数字、字符串、元组、列表、字典等 Python 字面量；
3. 都失败则按原字符串保存。

因此 `-s DOWNLOAD_TIMEOUT=20` 得到 `int`，`-s USER_AGENT=pcrawl/2.0` 得到字符串。含空格的值请加引号。

### 设置模块解析

未传 `--settings-module` 时：

1. 读取环境变量 `PCRAWL_SETTINGS_MODULE`；
2. 否则读取当前目录 `pcrawl.cfg` 的 `[settings] default`；
3. 都没有则只用默认值。

### 输出

默认输出一行：

```text
pcrawl finished: {'task_received': 10, 'item_scraped': 10, ...}
```

`--json` 输出缩进、排序后的 JSON：

```json
{
  "item_scraped": 10,
  "request_received": 10,
  "request_succeeded": 10,
  "task_received": 10,
  "task_succeeded": 10
}
```

日志通过 `logging.basicConfig` 写入 stderr，格式包含时间、级别、进程号与 logger 名。

### 退出码

| 码 | 场景 |
| --- | --- |
| `0` | 运行结束（包括 `CloseSpider` 主动关闭） |
| `2` | 参数错误（argparse） |
| 非零 | 未捕获异常，traceback 写入 stderr |

### Windows / 进程模式

`process` 模式使用 `spawn` 启动子进程，会重新导入主模块。通过 CLI 运行时 Spider 类必须位于可导入模块顶层；不要定义在 `__main__` 或函数内部。

## 示例

```bash
# 最小运行
pcrawl run examples.simple_http:SimpleSpider

# 多进程 + Redis
pcrawl run myproject.spiders.StoreSpider \
  --workers 8 \
  --queue redis \
  --redis-url redis://redis:6379/0 \
  --purser local \
  --json

# 常驻 worker，等待外部投递任务
pcrawl run myproject.spiders.WorkerSpider --keep-alive --queue redis
```

Python 内等价调用：

```python
from myproject.spiders import MySpider
from pcrawl import CrawlerEngine

stats = CrawlerEngine(MySpider, {"workers": 4}).run()
```

`CrawlerEngine` 的第一个参数是 Spider 类或实例，不是导入路径字符串；字符串路径只用于 CLI。
