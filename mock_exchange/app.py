"""本地模拟行情源（无需账号、无需外网）。

模拟 Binance Spot 的两类接口：
  - HTTP 深度快照:  GET /api/v3/depth?symbol=BTCUSDT&limit=100
  - WebSocket 增量: WS  /ws/btcusdt@depth   (depthUpdate, 字段 U/u/b/a)

内置故障场景开关（POST /scenario）：
  snapshot_delay_ms  快照响应延迟（先按请求时刻取数，再延迟返回，模拟在途慢响应）
  snapshot_fail      快照返回 HTTP 500
  duplicates         每条增量消息重复发送一次
  gap                周期性跳过更新编号（制造缺号）
  disconnect         周期性关闭全部 WebSocket 连接
  close_stall        关闭握手不响应：客户端发出 close 帧后连接陷入静默

运行方式：
  python app.py            # 对外端口 8001（TCP 中继）→ 本机 uvicorn 8011

说明：uvicorn 的 WebSocket 协议层会自动回复 close 帧，无法在应用内模拟
「关闭握手不响应」。因此本进程在对外端口上运行一个极简 TCP 中继，原样转发
HTTP/WS 流量；仅当 close_stall 开启且客户端发出 WS close 帧时，中继进入
静默（双向停止转发但保持连接），使采集端的关闭握手迟迟得不到回应。
直接以 `uvicorn app:app --port 8011` 调试可绕过中继（无 close_stall 效果）。
"""
from __future__ import annotations

import asyncio
import os
import random
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel

SYMBOL = "BTCUSDT"
TICK = 0.1
MAX_BAND = 150          # 模拟盘口维持在中间价 ±150 个 tick
SNAPSHOT_LIMIT = 100
TICK_INTERVAL = 0.2     # 增量推送间隔（秒）

APP_HOST = "127.0.0.1"
APP_PORT = int(os.getenv("APP_PORT", "8011"))        # uvicorn 实际监听（仅本机内部）
PUBLIC_PORT = int(os.getenv("PUBLIC_PORT", "8001"))  # 对外端口（TCP 中继）

WS_CLOSE_OPCODE = 0x8


class Scenario(BaseModel):
    snapshot_delay_ms: int = 0
    snapshot_fail: bool = False
    duplicates: bool = False
    gap: bool = False
    disconnect: bool = False
    close_stall: bool = False  # 关闭握手不响应


scenario = Scenario()

mid_price = 42000.0
bids: dict[float, float] = {}
asks: dict[float, float] = {}
next_update_id = 1
event_count = 0
clients: set[WebSocket] = set()
lock = asyncio.Lock()


def seed_book() -> None:
    for i in range(1, MAX_BAND + 1):
        bids[round(mid_price - i * TICK, 1)] = round(random.uniform(0.05, 8.0), 4)
        asks[round(mid_price + i * TICK, 1)] = round(random.uniform(0.05, 8.0), 4)


def mutate_book() -> tuple[list[list[str]], list[list[str]]]:
    """对模拟盘口做 1~3 笔随机修改，返回 diff depth 的 b/a 变更列表。"""
    global mid_price
    if random.random() < 0.02:  # 中间价缓慢漂移
        mid_price = round(mid_price + random.choice((-1, 1)) * TICK, 1)
        # 漂移后清理穿越档，保持买侧 < 卖侧
        for p in [p for p in bids if p >= mid_price]:
            bids.pop(p)
        for p in [p for p in asks if p <= mid_price]:
            asks.pop(p)
    b_changes: list[list[str]] = []
    a_changes: list[list[str]] = []
    for _ in range(random.randint(1, 3)):
        side = random.choice(("b", "a"))
        offset = random.randint(1, MAX_BAND)
        price = round(mid_price - offset * TICK, 1) if side == "b" else round(mid_price + offset * TICK, 1)
        book = bids if side == "b" else asks
        if random.random() < 0.12:
            qty = 0.0  # 数量为 0 表示删除该价位
            book.pop(price, None)
        else:
            qty = round(random.uniform(0.01, 8.0), 4)
            book[price] = qty  # 数量为该价位的最新总量（绝对值）
        (b_changes if side == "b" else a_changes).append([f"{price:.1f}", f"{qty:.4f}"])
    # 清理远离中间价的档位
    for p in [p for p in bids if p < mid_price - MAX_BAND * TICK]:
        bids.pop(p)
    for p in [p for p in asks if p > mid_price + MAX_BAND * TICK]:
        asks.pop(p)
    return b_changes, a_changes


async def broadcast(event: dict) -> None:
    dead = []
    for ws in list(clients):
        try:
            await ws.send_json(event)
            if scenario.duplicates:
                await ws.send_json(event)  # 重复消息场景：同一条再发一遍
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


async def close_all_clients() -> None:
    for ws in list(clients):
        try:
            await ws.close(code=1011, reason="mock disconnect scenario")
        except Exception:
            pass
    clients.clear()


async def market_loop() -> None:
    global next_update_id, event_count
    while True:
        await asyncio.sleep(TICK_INTERVAL)
        async with lock:
            b_changes, a_changes = mutate_book()
            event_count += 1
            if scenario.gap and event_count % 12 == 0:
                # 缺号场景：跳过 2~5 个更新编号，采集端将看到 U > L+1
                next_update_id += random.randint(2, 5)
            event = {
                "e": "depthUpdate",
                "E": int(time.time() * 1000),
                "s": SYMBOL,
                "U": next_update_id,  # 本事件起始更新编号
                "u": next_update_id,  # 本事件结束更新编号
                "b": b_changes,
                "a": a_changes,
            }
            next_update_id += 1
            await broadcast(event)
            if scenario.disconnect and event_count % 40 == 0:
                # 断线场景：广播后关闭全部连接
                await close_all_clients()


@asynccontextmanager
async def lifespan(_: FastAPI):
    seed_book()
    task = asyncio.create_task(market_loop())
    yield
    task.cancel()


app = FastAPI(title="Mock Binance Spot Market Source", lifespan=lifespan)


@app.get("/api/v3/depth")
async def depth(symbol: str = SYMBOL, limit: int = SNAPSHOT_LIMIT):
    """Binance Spot GET /api/v3/depth 风格快照。

    先在锁内按当前时刻取数（lastUpdateId = 已生成的最大更新编号），
    再按场景配置延迟返回，用于验证采集端“先缓存增量再对齐快照”的逻辑。
    """
    async with lock:
        snapshot = {
            "lastUpdateId": next_update_id - 1,
            "bids": [[f"{p:.1f}", f"{q:.4f}"]
                     for p, q in sorted(bids.items(), key=lambda kv: kv[0], reverse=True)[:limit]],
            "asks": [[f"{p:.1f}", f"{q:.4f}"]
                     for p, q in sorted(asks.items(), key=lambda kv: kv[0])[:limit]],
        }
        delay_ms = scenario.snapshot_delay_ms
        fail = scenario.snapshot_fail
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000)
    if fail:
        return JSONResponse(status_code=500, content={"code": -1000, "msg": "mock snapshot failure"})
    return snapshot


@app.websocket("/ws/{stream}")
async def ws_endpoint(websocket: WebSocket, stream: str):
    await websocket.accept()
    clients.add(websocket)
    try:
        while True:
            await websocket.receive_text()  # 客户端不发送数据，仅用于感知断开
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        clients.discard(websocket)


@app.get("/scenario")
async def get_scenario():
    return scenario.model_dump()


@app.post("/scenario")
async def set_scenario(update: Scenario):
    global scenario
    scenario = update
    return scenario.model_dump()


@app.get("/")
async def root():
    return {
        "name": "mock-binance-spot",
        "symbol": SYMBOL,
        "snapshot": f"/api/v3/depth?symbol={SYMBOL}&limit={SNAPSHOT_LIMIT}",
        "websocket": f"/ws/{SYMBOL.lower()}@depth",
        "scenario": scenario.model_dump(),
    }


# ------------------------- 关闭握手不响应场景（TCP 中继） -------------------------


class CloseFrameScanner:
    """增量解析客户端→服务端的 WS 帧，发现 close 帧（opcode 0x8）即报告。

    只需定位帧边界：帧头 2 字节 + 扩展长度 + 掩码键，随后是载荷。
    """

    def __init__(self) -> None:
        self.buf = bytearray()
        self.found = False

    def feed(self, data: bytes) -> bool:
        if self.found:
            return True
        self.buf += data
        while True:
            if len(self.buf) < 2:
                return False
            b1 = self.buf[1]
            length = b1 & 0x7F
            header = 2 + (4 if b1 & 0x80 else 0)  # 掩码键 4 字节
            if length == 126:
                header += 2
            elif length == 127:
                header += 8
            if len(self.buf) < header:
                return False
            if length == 126:
                length = int.from_bytes(self.buf[2:4], "big")
            elif length == 127:
                length = int.from_bytes(self.buf[2:10], "big")
            total = header + length
            if len(self.buf) < total:
                return False
            if self.buf[0] & 0x0F == WS_CLOSE_OPCODE:
                self.found = True
                return True
            del self.buf[:total]


async def relay_connection(client_reader: asyncio.StreamReader,
                           client_writer: asyncio.StreamWriter) -> None:
    """转发 客户端↔uvicorn 流量；close_stall 开启时，客户端 close 帧之后进入静默。

    静默期间双向字节一律丢弃、两侧 TCP 连接均保持打开：上游应答 close 帧后
    会主动关闭 TCP，但该 EOF 不得传播给客户端，否则客户端会把它当作握手完成。
    """
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(APP_HOST, APP_PORT)
    except OSError:
        client_writer.close()
        return

    stalled = False  # 静默中：双向字节一律丢弃，但继续读取以排空缓冲

    async def client_to_upstream() -> None:
        nonlocal stalled
        request = bytearray()
        is_ws: bool | None = None  # None=请求头未收齐
        scanner = CloseFrameScanner()
        try:
            while True:
                data = await client_reader.read(65536)
                if not data:
                    return
                if stalled:
                    continue  # 静默：丢弃客户端后续字节
                if is_ws is None:
                    request += data
                    if b"\r\n\r\n" not in request:
                        continue  # 等请求头收齐，以判定是否为 WS 升级
                    headers, _, rest = bytes(request).partition(b"\r\n\r\n")
                    is_ws = b"upgrade: websocket" in headers.lower()
                    upstream_writer.write(headers + b"\r\n\r\n" + rest)
                    await upstream_writer.drain()
                    if not is_ws:
                        continue  # 普通 HTTP：此后原样透传
                    if rest and scanner.feed(rest) and scenario.close_stall:
                        stalled = True
                    continue
                if is_ws and scanner.feed(data) and scenario.close_stall:
                    # 触发静默：本帧（含 close 帧）仍送达，此后双向不再转发
                    stalled = True
                upstream_writer.write(data)
                await upstream_writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            return

    async def upstream_to_client() -> None:
        try:
            while True:
                data = await upstream_reader.read(65536)
                if not data:
                    return
                if stalled:
                    continue  # 静默：丢弃服务端字节（含 close 帧回应）
                client_writer.write(data)
                await client_writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            return

    c2u = asyncio.create_task(client_to_upstream())
    u2c = asyncio.create_task(upstream_to_client())
    done, _ = await asyncio.wait({c2u, u2c}, return_when=asyncio.FIRST_COMPLETED)
    if c2u in done:
        u2c.cancel()  # 客户端断开：结束转发
    elif stalled:
        # 静默中上游先关闭（应答 close 后主动断 TCP）：保持客户端连接，
        # 等客户端放弃（采集端关闭握手超时后强制断开），不得传播该 EOF
        await c2u
        u2c.cancel()
    else:
        c2u.cancel()  # 常规上游断开：结束转发
    await asyncio.gather(c2u, u2c, return_exceptions=True)
    for writer in (client_writer, upstream_writer):
        try:
            writer.close()
        except OSError:
            pass


async def main() -> None:
    relay = await asyncio.start_server(relay_connection, "0.0.0.0", PUBLIC_PORT)
    config = uvicorn.Config(app, host=APP_HOST, port=APP_PORT, log_level="info")
    server = uvicorn.Server(config)
    async with relay:
        server_task = asyncio.create_task(server.serve())
        relay_task = asyncio.create_task(relay.serve_forever())
        # uvicorn 收到退出信号（或启动失败）即结束，随后关停中继，进程整体退出
        await asyncio.wait({server_task, relay_task}, return_when=asyncio.FIRST_COMPLETED)
        relay_task.cancel()
        if not server_task.done():
            server.should_exit = True
        await asyncio.gather(server_task, relay_task, return_exceptions=True)
        if server_task.done() and not server_task.cancelled():
            server_task.result()  # 启动失败等异常向上抛出


if __name__ == "__main__":
    asyncio.run(main())
