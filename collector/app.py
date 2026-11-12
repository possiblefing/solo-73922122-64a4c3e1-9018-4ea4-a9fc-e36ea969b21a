"""深度行情采集服务（FastAPI 单进程）。

对齐 Binance Spot 本地盘口维护规范：
  1. 先连 WebSocket 缓存增量 depthUpdate，再取 HTTP 深度快照；
  2. 快照 lastUpdateId 记为 L，增量含起止编号 U/u；
  3. 丢弃 u <= L 的增量；仅当 U <= L+1 <= u 时应用，并令 L = u；
  4. 增量中的数量是该价位的最新总量，0 表示删除该价位；每侧最多保留 100 档；
  5. 缺号（U > L+1）或接收任务结束：先撤下报价与编号、展示故障原因，再清理连接；
  6. 关闭握手最多等待 CLOSE_TIMEOUT 秒，超时强制断开；本周期接收/快照任务
     全部结束、连接清理完毕后才自动重同步，不遗留后台清理；
  7. 清理期间停采不再重连；快照失败绝不上报就绪；重连前的在途快照响应
     不得覆盖新盘口（代际号防护）。
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import httpx
import websockets
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
from websockets.exceptions import WebSocketException

SYMBOL = "BTCUSDT"
MAX_LEVELS = 100
MOCK_HTTP = os.getenv("MOCK_HTTP", "http://localhost:8001")
MOCK_WS = os.getenv("MOCK_WS", "ws://localhost:8001/ws/btcusdt@depth")
SNAPSHOT_TIMEOUT = 30.0
RESYNC_BACKOFF = 1.0
CLOSE_TIMEOUT = 1.0  # 关闭握手最多等待 1 秒，超时强制断开底层连接

STATIC_INDEX = Path(__file__).parent / "static" / "index.html"


class GapDetected(Exception):
    """增量编号与本地 L 不连续（U > L+1）。"""


class ResyncRequired(Exception):
    """连接中断等需要重新同步的情况。"""


class OrderBook:
    """本地盘口：数量为价位最新总量，0 删除；每侧最多 MAX_LEVELS 档。"""

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_update_id: int | None = None  # L

    def load_snapshot(self, snapshot: dict) -> None:
        self.bids = {float(p): float(q) for p, q in snapshot["bids"]}
        self.asks = {float(p): float(q) for p, q in snapshot["asks"]}
        self.last_update_id = int(snapshot["lastUpdateId"])
        self._trim()

    def apply(self, event: dict, stats: dict) -> None:
        U, u = int(event["U"]), int(event["u"])
        L = self.last_update_id
        assert L is not None, "快照未加载"
        if u <= L:
            stats["dropped"] += 1  # 重复/过期消息，直接丢弃
            return
        if U > L + 1:
            raise GapDetected(f"缺号: U={U} > L+1={L + 1}")
        # 满足 U <= L+1 <= u，应用并推进 L
        for p, q in event.get("b", []):
            self._set(self.bids, float(p), float(q))
        for p, q in event.get("a", []):
            self._set(self.asks, float(p), float(q))
        self.last_update_id = u
        stats["applied"] += 1
        self._trim()

    @staticmethod
    def _set(side: dict[float, float], price: float, qty: float) -> None:
        if qty == 0.0:
            side.pop(price, None)
        else:
            side[price] = qty

    def _trim(self) -> None:
        if len(self.bids) > MAX_LEVELS:
            self.bids = dict(sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:MAX_LEVELS])
        if len(self.asks) > MAX_LEVELS:
            self.asks = dict(sorted(self.asks.items(), key=lambda kv: kv[0])[:MAX_LEVELS])

    def top(self, n: int) -> tuple[list[list[str]], list[list[str]]]:
        bids = [[f"{p:.1f}", f"{q:.4f}"]
                for p, q in sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:n]]
        asks = [[f"{p:.1f}", f"{q:.4f}"]
                for p, q in sorted(self.asks.items(), key=lambda kv: kv[0])[:n]]
        return bids, asks


class CollectorState:
    def __init__(self) -> None:
        self.running = False
        self.status = "stopped"               # stopped / syncing / synced / resyncing
        self.book: OrderBook | None = None    # 仅在 synced 时对外发布，其余时间为 None
        self.generation = 0                   # 代际号：防止在途旧响应覆盖新盘口
        self.task: asyncio.Task | None = None
        self.last_error: str | None = None
        self.stats = {
            "applied": 0,            # 已应用增量
            "dropped": 0,            # 丢弃的重复/过期消息
            "gaps": 0,               # 缺号次数
            "resyncs": 0,            # 重同步次数
            "snapshotAttempts": 0,   # 快照尝试次数
            "snapshotOk": 0,         # 快照成功次数
        }


state = CollectorState()


def describe_error(exc: Exception) -> str:
    """故障原因分类，供页面与 /api/state 展示。"""
    if isinstance(exc, GapDetected):
        return str(exc)
    if isinstance(exc, (ResyncRequired, WebSocketException, OSError)):
        return f"连接问题: {exc}"
    if isinstance(exc, httpx.HTTPError):
        return f"快照失败: {exc}"
    return f"未预期错误: {exc!r}"


async def fetch_snapshot() -> dict:
    state.stats["snapshotAttempts"] += 1
    async with httpx.AsyncClient(timeout=SNAPSHOT_TIMEOUT) as client:
        resp = await client.get(
            f"{MOCK_HTTP}/api/v3/depth",
            params={"symbol": SYMBOL, "limit": MAX_LEVELS},
        )
        resp.raise_for_status()  # 快照失败：抛错，外层重试，期间不就绪
        data = resp.json()
    state.stats["snapshotOk"] += 1
    return data


async def close_connection(ws) -> None:
    """关闭握手最多等待 CLOSE_TIMEOUT 秒，超时强制断开底层传输。

    停采取消（CancelledError）不打断关闭流程，但总预算仍是 CLOSE_TIMEOUT；
    无论何种路径返回，底层连接都不会遗留为半开状态。
    """
    close_task = asyncio.ensure_future(ws.close())
    deadline = asyncio.get_running_loop().time() + CLOSE_TIMEOUT
    try:
        while not close_task.done():
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                ws.transport.abort()  # 关闭握手超时：强制断开
                break
            try:
                # shield：超时/取消只打断等待，不扼杀 close 协程本身
                await asyncio.wait_for(asyncio.shield(close_task), timeout=remaining)
            except asyncio.TimeoutError:
                ws.transport.abort()  # 关闭握手无响应：强制断开
            except asyncio.CancelledError:
                continue  # 停采取消不延长清理，下一轮循环即触及 deadline
            except (WebSocketException, OSError):
                break  # 连接已断开或握手已完成
    finally:
        if close_task.done():
            if not close_task.cancelled():
                _ = close_task.exception()  # 取回异常，避免 "never retrieved" 告警
        else:
            close_task.cancel()


async def teardown_cycle(ws, *tasks: asyncio.Task | None) -> None:
    """清理本周期：取消并等待接收/快照任务全部结束，再关闭连接。

    停采取消不打断清理，确保不遗留后台任务与半开连接。
    """
    pending = [t for t in tasks if t is not None and not t.done()]
    for t in pending:
        t.cancel()
    while pending:
        try:
            await asyncio.wait(pending)  # 本周期任务全部结束后才返回
            break
        except asyncio.CancelledError:
            continue  # 停采取消不中断清理
    # 取回全部任务异常（含清理前已结束的），避免 "never retrieved" 告警
    for t in tasks:
        if t is not None and t.done() and not t.cancelled():
            _ = t.exception()
    if ws is not None:
        await close_connection(ws)


async def sync_cycle() -> None:
    """一轮「连接 → 缓存增量 → 取快照 → 对齐 → 直播」周期。

    处理顺序（修复要点）：检测到缺号或接收任务结束时，先撤下报价与编号、
    记录故障原因，再清理连接；本周期任务全部结束、连接清理完毕后才返回，
    由外层决定是否重同步。
    """
    state.generation += 1
    gen = state.generation
    state.status = "syncing"

    buffer: list[dict] = []
    closed = asyncio.Event()
    item_ready = asyncio.Event()

    async def pump(ws) -> None:
        try:
            async for raw in ws:
                buffer.append(json.loads(raw))
                item_ready.set()
        finally:
            closed.set()
            item_ready.set()

    ws = None
    pump_task: asyncio.Task | None = None
    fetch_task: asyncio.Task | None = None
    try:
        ws = await websockets.connect(
            MOCK_WS, ping_interval=20, ping_timeout=20, close_timeout=CLOSE_TIMEOUT
        )
        pump_task = asyncio.create_task(pump(ws))
        # 步骤一：pump 已在缓存 WebSocket 增量；步骤二：并发取 HTTP 快照
        fetch_task = asyncio.create_task(fetch_snapshot())
        # 等待快照返回或接收任务结束（连接断开），先到先处理
        await asyncio.wait({fetch_task, pump_task}, return_when=asyncio.FIRST_COMPLETED)
        if pump_task.done():
            # 快照在途期间断线：作废本次在途响应，直接重同步
            raise ResyncRequired("快照在途期间连接断开")
        snapshot = fetch_task.result()  # 快照失败在此抛错，绝不将就绪置真
        if gen != state.generation:
            return  # 在途旧响应不得覆盖新盘口

        book = OrderBook()
        book.load_snapshot(snapshot)

        # 步骤三：对齐缓存增量，随后进入直播
        i = 0
        published = False
        while True:
            while i < len(buffer):
                book.apply(buffer[i], state.stats)  # 缺号抛 GapDetected
                i += 1
            if closed.is_set():
                raise ResyncRequired("连接断开")
            if not published:
                if gen != state.generation:
                    return  # 在途旧周期不得发布
                state.book = book  # 对齐完成，开始发布有效报价
                state.status = "synced"
                state.last_error = None
                published = True
            item_ready.clear()
            if i < len(buffer) or closed.is_set():
                continue
            await item_ready.wait()
    except asyncio.CancelledError:
        raise  # 停采：finally 完成清理后由外层收尾
    except Exception as exc:  # noqa: BLE001
        # 修复要点一：先撤下报价与编号、展示故障原因，再清理连接
        if gen == state.generation:
            state.book = None
            state.last_error = describe_error(exc)
            if state.running:
                state.status = "resyncing"
        raise
    finally:
        # 修复要点二：本周期接收/快照任务全部结束、连接清理完毕后才返回
        await teardown_cycle(ws, pump_task, fetch_task)


async def collector_loop() -> None:
    while state.running:
        try:
            await sync_cycle()
        except asyncio.CancelledError:
            raise
        except GapDetected:
            state.stats["gaps"] += 1
        except Exception:  # noqa: BLE001
            pass  # 故障原因已在 sync_cycle 内记录
        # 缺号/断线/快照失败：sync_cycle 已在清理前停止发布；此处仅统计与退避
        if state.running:
            state.status = "resyncing"
            state.stats["resyncs"] += 1
            await asyncio.sleep(RESYNC_BACKOFF)
    state.status = "stopped"
    state.book = None


async def start_collection() -> None:
    if state.running:
        return
    state.running = True
    state.last_error = None
    state.task = asyncio.create_task(collector_loop())


async def stop_collection() -> None:
    state.running = False
    if state.task is not None:
        state.task.cancel()
        try:
            await state.task
        except asyncio.CancelledError:
            pass
        state.task = None
    state.status = "stopped"
    state.book = None


# ------------------------------- HTTP API -------------------------------

app = FastAPI(title="Depth Collector")


class ControlBody(BaseModel):
    action: str  # start / stop


class ScenarioBody(BaseModel):
    snapshot_delay_ms: int = 0
    snapshot_fail: bool = False
    duplicates: bool = False
    gap: bool = False
    disconnect: bool = False
    close_stall: bool = False  # 关闭握手不响应（行情源对 close 帧不作答）


@app.get("/")
async def index():
    return FileResponse(STATIC_INDEX)


@app.post("/api/control")
async def api_control(body: ControlBody):
    if body.action == "start":
        await start_collection()
    elif body.action == "stop":
        await stop_collection()
    else:
        return JSONResponse(status_code=400, content={"error": "action 须为 start/stop"})
    return {"ok": True, "running": state.running}


@app.get("/api/state")
async def api_state():
    synced = state.status == "synced" and state.book is not None
    if synced:
        bids, asks = state.book.top(5)
        last_update_id = state.book.last_update_id
        levels = {"bids": len(state.book.bids), "asks": len(state.book.asks)}
    else:
        # 未同步：不发布任何报价，下游不得使用
        bids, asks, last_update_id, levels = [], [], None, {"bids": 0, "asks": 0}
    return {
        "running": state.running,
        "status": state.status,
        "synced": synced,
        "symbol": SYMBOL,
        "lastUpdateId": last_update_id,
        "bids": bids,
        "asks": asks,
        "levels": levels,
        "stats": state.stats,
        "lastError": state.last_error,
        "source": {"http": MOCK_HTTP, "ws": MOCK_WS},
    }


@app.get("/api/scenario")
async def api_get_scenario():
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(f"{MOCK_HTTP}/scenario")
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as exc:
        return JSONResponse(status_code=502, content={"error": f"行情源不可达: {exc}"})


@app.post("/api/scenario")
async def api_set_scenario(body: ScenarioBody):
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(f"{MOCK_HTTP}/scenario", json=body.model_dump())
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPError as exc:
        return JSONResponse(status_code=502, content={"error": f"行情源不可达: {exc}"})
