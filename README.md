# 深度行情采集演示（Binance Spot 快照 + diff depth）

面向行情数据工程的断流核查演示：单进程采集端接入**本地模拟行情源**（无需账号、无需外网），
按 Binance Spot 官方规范维护本地盘口。检测到缺号或连接结束时**先撤下报价与编号、展示故障
原因，再清理连接**；关闭握手最多等待 1 秒，超时强制断开；本周期任务全部结束后才自动重同步。
浏览器控制台可启动采集、查看买卖前五档与同步状态，并一键组合故障场景。

## 架构

```
浏览器 ──► collector (FastAPI, :8000) ──WS 增量──► mock_exchange (:8001)
              │  └ 静态控制台 /              ├ TCP 中继 :8001（关闭握手不响应场景）
              │     REST API                 └ uvicorn   :8011（仅容器/本机内部）
              └ 维护本地盘口（每侧≤100档）       ├ GET /api/v3/depth   HTTP 深度快照
                                               ├ WS  /ws/btcusdt@depth  diff depth 增量
                                               └ GET/POST /scenario   故障场景开关
```

- **mock_exchange**：本地模拟行情源。HTTP 快照（`lastUpdateId` + `bids/asks`）与
  WebSocket 增量（`U/u/b/a` 字段，Binance `depthUpdate` 风格），内置 6 个故障场景。
  单进程内同时运行：对外 TCP 中继（:8001）与本机 uvicorn（:8011）。中继原样转发
  HTTP/WS 流量；仅当 `close_stall` 开启且客户端发出 WS close 帧时进入静默（双向停转、
  连接保持），模拟「关闭握手不响应」的行情源。
- **collector**：采集端。先缓存 WS 增量再取 HTTP 快照，对齐后发布有效报价；
  固定交易对 `BTCUSDT`，每侧最多 100 档。

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000  （采集控制台，浏览器入口）
# 行情源调试入口 http://localhost:8001 （可查看 /api/v3/depth 与 /scenario）
```

无 Docker 时本地运行（Python 3.11+）：

```bash
pip install -r mock_exchange/requirements.txt -r collector/requirements.txt
python mock_exchange/app.py &          # 中继 :8001 + uvicorn :8011
MOCK_HTTP=http://localhost:8001 MOCK_WS=ws://localhost:8001/ws/btcusdt@depth \
  uvicorn app:app --port 8000 --app-dir collector &
```

> 直接 `uvicorn app:app --port 8011 --app-dir mock_exchange` 可绕过中继调试行情源
> （此时无 `close_stall` 效果）。

## 盘口同步规则（与代码对照）

| 规则 | 实现 |
|---|---|
| 先缓存 WebSocket 增量，再取 HTTP 快照 | `collector/app.py` `sync_cycle()`：`pump` 先运行，`fetch_snapshot()` 并发进行 |
| 快照编号为 L，丢弃 `u ≤ L` 的增量 | `OrderBook.apply()`：`u <= L` → 计入 `dropped` 并丢弃 |
| 仅当 `U ≤ L+1 ≤ u` 时应用并令 `L = u` | `OrderBook.apply()`：`U > L+1` 抛 `GapDetected`，否则应用并推进 L |
| 数量是价位最新总量，0 表示删除 | `OrderBook._set()`：绝对值覆盖，`qty == 0` 删除价位 |
| 每侧最多 100 档 | `OrderBook._trim()`：保留最优 100 档 |
| 缺号/接收结束：先撤报价再清理连接 | `sync_cycle()` 异常分支：先 `state.book = None` + 记录故障原因，再进 `finally` 清理 |
| 关闭握手最多 1 秒，超时强制断开 | `close_connection()`：`CLOSE_TIMEOUT=1.0` 预算，超时 `transport.abort()` |
| 本周期任务全部结束才重同步 | `teardown_cycle()`：取消并等待接收/快照任务、关闭连接后才返回，不留后台清理 |
| 清理期间停采不再重连 | `collector_loop()` 退出前检查 `state.running`；清理不被停采取消打断 |
| 快照失败不能报就绪 | `fetch_snapshot()`：`raise_for_status()`，失败周期全程 `synced=false` |
| 在途响应不得覆盖新盘口 | 断线即取消在途快照请求 + `generation` 代际号双重防护 |

## 控制台使用

1. 点击 **启动采集** —— 状态变为「同步中（缓存增量/取快照）」，对齐后变为「已同步 · 发布有效报价」，
   展示买卖前五档、`lastUpdateId (L)`、档位数与统计计数。
2. 故障场景开关（作用于行情源，即改即生效）：
   - **快照延迟**：快照 N ms 后才返回。延迟期间状态保持「同步中」，不就绪；恢复后自动对齐。
   - **快照失败**：快照返回 500。期间状态绝不变为「已同步」，报价停止发布，快照计数持续重试。
   - **重复消息**：每条增量发两遍。「丢弃（重复/过期）」计数上升，盘口不受影响。
   - **缺号注入**：周期性跳过更新编号。「缺号次数」上升，报价立即撤下，自动重同步后恢复。
   - **断线注入**：周期性关闭 WS 连接。「重同步次数」上升，自动重连重对齐。
   - **关闭握手不响应**：采集端关闭连接时行情源对 close 帧不作答。配合缺号/断线使用。
3. **组合演示（本次修复的核心场景）**：勾选「缺号注入」+「关闭握手不响应」——
   缺号后报价与编号**立即**撤下（状态变「重新同步中」并显示故障原因），卡住的关闭握手
   约 1 秒被强制断开，随后自动重同步恢复报价。修复前在此场景下旧盘口会被标为可用约 10 秒
   （websockets 默认 `close_timeout`），网页与 `/api/state` 均可见。

## API（采集端，:8000）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/` | 控制台页面 |
| POST | `/api/control` | `{"action":"start"\|"stop"}` 启动/停止采集 |
| GET | `/api/state` | 运行状态、L、前五档、档位数、统计、最近异常（未同步时不含报价） |
| GET/POST | `/api/scenario` | 查询/设置行情源故障场景（透传，含 `close_stall`） |

行情源（:8001）：`GET /api/v3/depth?symbol=BTCUSDT&limit=100`、`WS /ws/btcusdt@depth`、`GET/POST /scenario`。

## 测试

```bash
cd collector && python -m pytest test_order_book.py -v
```

覆盖：`u ≤ L` 丢弃、`U ≤ L+1 ≤ u` 应用并推进 L、缺号抛错、零量删除、数量为绝对值、
100 档裁剪；以及本次修复的回归测试——关闭握手不响应时 1 秒强制断开、缺号/接收结束时
先撤报价再清理、清理期间停采不重连。
