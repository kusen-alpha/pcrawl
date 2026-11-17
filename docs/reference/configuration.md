# 配置项

完整合并规则与示例见 [配置分层](../guides/configuration.md)。本页是字段速查。

## 加载顺序

```python
Settings.load(
    settings_module=None,
    *,
    overrides=None,
    base=None,
    use_env=True,
    config_file="pcrawl.cfg",
)
```

默认值 → 设置模块（参数 / `PCRAWL_SETTINGS_MODULE` / `pcrawl.cfg`）→ `overrides`。

`CrawlerEngine` 在此基础上继续叠加：

1. `Spider.settings` 的显式值；
2. `Spider.custom_settings`；
3. `CrawlerEngine(settings={...})`（CLI 参数也在此层）。

## 字段

| 字段 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| `name` | `str` | `pcrawl` | worker id 前缀 |
| `workers` | `int` | `1` | thread 线程数 / process 进程数 |
| `concurrency` | `int` | `1` | async runtime 数 |
| `idle_timeout` | `float` | `2.0` | 已开会话中持续无可领取条目时的等待秒数 |
| `keep_alive` | `bool` | `False` | worker 是否常驻 |
| `queue_backend` | `"memory" \| "redis"` | `memory` | 队列后端 |
| `queue_order` | `"fifo" \| "priority"` | `fifo` | 出队顺序 |
| `redis_url` | `str` | `redis://localhost:6379/0` | Redis 地址 |
| `redis_key` | `str` | `pcrawl` | key 前缀 |
| `visibility_timeout` | `float` | `300.0` | Redis 在途可见性超时 |
| `claim_poll_interval` | `float` | `0.2` | 空队列轮询间隔 |
| `task_max_retries` | `int` | `3` | task 最大重试次数 |
| `task_retry_delay` | `float` | `1.0` | task 重试延迟 |
| `request_max_retries` | `int` | `2` | request 最大重试次数 |
| `request_retry_delay` | `float` | `1.0` | request 基础延迟 |
| `request_retry_http_codes` | `tuple[int, ...]` | `(429, 500, 502, 503, 504)` | 触发重试的状态码 |
| `request_retry_priority_adjust` | `int` | `-1` | 重试优先级调整 |
| `request_retry_max_delay` | `float` | `60.0` | `Retry-After` 上限 |
| `request_dedup_ttl` | `float` | `0.0` | 去重 TTL；0 为永久 |
| `download_timeout` | `float` | `30.0` | 下载超时 |
| `user_agent` | `str` | `pcrawl/0.1` | 默认 UA |
| `trust_env` | `bool` | `False` | httpx 是否读取代理环境变量 |
| `shutdown_timeout` | `float` | `30.0` | process 优雅退出等待 |
| `downloader_middlewares` | 任意组件声明 | `()` | 下载器中间件 |
| `spider_middlewares` | 任意组件声明 | `()` | Spider 中间件 |
| `item_pipelines` | 任意组件声明 | `()` | Item Pipeline |
| `purser_mode` | `"auto" \| "off" \| "embedded" \| "local"` | `auto` | 资源后端选择 |
| `purser_pools` | `dict[str, dict]` | `{}` | 池声明，和 `start_resources` 合并 |
| `purser_storage` | `str` | `memory` | purser 存储层 |
| `purser_coordination` | `str` | `memory` | purser 协调层 |
| `purser_sqlite_path` | `str \| None` | `None` | SQLite 路径 |
| `purser_redis_url` | `str \| None` | `None` | purser Redis 地址 |
| `purser_start_method` | `str` | `spawn` | LocalScheduler 启动方式 |
| `resource_wait_timeout` | `float` | `30.0` | 会话资源等待超时；`<=0` 无限 |
| `lease_ttl` | `float` | `60.0` | 租约 TTL |
| `mp_start_method` | `str` | `spawn` | multiprocessing 启动方式 |
| `log_level` | `str` | `INFO` | 日志级别 |
| `stats_interval` | `float` | `0.0` | 保留字段，当前未实现周期输出 |

## Scrapy 别名

| 别名 | 字段 |
| --- | --- |
| `NAME` | `name` |
| `WORKERS` | `workers` |
| `CONCURRENCY` / `CONCURRENT_REQUESTS` | `concurrency` |
| `IDLE_TIMEOUT` | `idle_timeout` |
| `KEEP_ALIVE` | `keep_alive` |
| `QUEUE_ORDER` | `queue_order` |
| `REDIS_URL` | `redis_url` |
| `REDIS_KEY` | `redis_key` |
| `VISIBILITY_TIMEOUT` | `visibility_timeout` |
| `CLAIM_POLL_INTERVAL` | `claim_poll_interval` |
| `MAX_RETRIES` / `RETRY_TIMES` | `request_max_retries` |
| `RETRY_DELAY` | `request_retry_delay` |
| `TASK_MAX_RETRIES` | `task_max_retries` |
| `TASK_RETRY_DELAY` | `task_retry_delay` |
| `RETRY_HTTP_CODES` | `request_retry_http_codes` |
| `RETRY_PRIORITY_ADJUST` | `request_retry_priority_adjust` |
| `RETRY_MAX_DELAY` | `request_retry_max_delay` |
| `REQUEST_DEDUP_TTL` | `request_dedup_ttl` |
| `DOWNLOAD_TIMEOUT` | `download_timeout` |
| `USER_AGENT` | `user_agent` |
| `TRUST_ENV` | `trust_env` |
| `SHUTDOWN_TIMEOUT` | `shutdown_timeout` |
| `DOWNLOADER_MIDDLEWARES` | `downloader_middlewares` |
| `SPIDER_MIDDLEWARES` | `spider_middlewares` |
| `ITEM_PIPELINES` | `item_pipelines` |
| `PURSER_MODE` | `purser_mode` |
| `PURSER_POOLS` | `purser_pools` |
| `PURSER_STORAGE` | `purser_storage` |
| `PURSER_COORDINATION` | `purser_coordination` |
| `PURSER_SQLITE_PATH` | `purser_sqlite_path` |
| `PURSER_REDIS_URL` | `purser_redis_url` |
| `PURSER_START_METHOD` | `purser_start_method` |
| `RESOURCE_WAIT_TIMEOUT` | `resource_wait_timeout` |
| `LEASE_TTL` | `lease_ttl` |
| `MP_START_METHOD` | `mp_start_method` |
| `LOG_LEVEL` | `log_level` |
| `STATS_INTERVAL` | `stats_interval` |

别名在设置模块、`Settings(**kwargs)`、`custom_settings` 与 `-s` 中都能识别；未识别的键保存在扩展区，可用 `get()` 读取。

## 校验规则

`validate()` 拒绝：

- 未知的 `queue_backend` / `queue_order` / `purser_mode`；
- `workers < 1`、`concurrency < 1`；
- 任一重试次数为负；
- `request_retry_delay < 0`、`request_retry_max_delay < request_retry_delay`；
- `request_dedup_ttl < 0`；
- 扩展区出现 `MODE` / `PCRAWL_MODE`。

构造引擎时还会拒绝 `thread` / `process` + 显式 `purser_mode="embedded"`。

## 池声明合并

`purser_pools` 的优先级低于 `start_resources` 返回的 `pools`，同名池会被后者覆盖：

```python
settings = Settings(
    purser_pools={
        "proxy": {
            "kind": "proxy",
            "config": {"max_slots": 50},
            "seed": [{"id": "p1", "url": "http://127.0.0.1:8080"}],
        }
    }
)
```

适合把静态池放在设置模块，把动态 seed 留在 Spider 里。
