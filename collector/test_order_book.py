"""盘口同步规则单元测试（不依赖网络，直接测试 OrderBook 纯逻辑）。

运行：cd collector && python -m pytest test_order_book.py -v
"""
import asyncio
import json
import time

import pytest

import app as collector
from app import GapDetected, OrderBook, state


def ev(U, u, b=None, a=None):
    return {"U": U, "u": u, "b": b or [], "a": a or []}


def snapshot(L=100):
    return {
        "lastUpdateId": L,
        "bids": [["100.0", "1.0000"], ["99.9", "2.0000"]],
        "asks": [["100.1", "1.5000"], ["100.2", "2.5000"]],
    }


def new_book():
    book = OrderBook()
    book.load_snapshot(snapshot())
    return book


def stats():
    return {"applied": 0, "dropped": 0}


def test_snapshot_loads_L_and_levels():
    book = new_book()
    assert book.last_update_id == 100
    assert book.bids == {100.0: 1.0, 99.9: 2.0}
    assert book.asks == {100.1: 1.5, 100.2: 2.5}


def test_drop_events_with_u_not_greater_than_L():
    book = new_book()
    s = stats()
    book.apply(ev(99, 100, b=[["100.0", "9.0000"]]), s)   # u == L，丢弃
    book.apply(ev(95, 98, b=[["100.0", "9.0000"]]), s)    # u < L，丢弃（重复消息）
    assert s["dropped"] == 2 and s["applied"] == 0
    assert book.bids[100.0] == 1.0  # 未被污染
    assert book.last_update_id == 100


def test_apply_straddling_event_and_advance_L():
    book = new_book()
    s = stats()
    book.apply(ev(99, 101, b=[["100.0", "3.0000"]]), s)   # U<=L+1<=u，应用
    assert book.bids[100.0] == 3.0
    assert book.last_update_id == 101
    book.apply(ev(102, 102, a=[["100.1", "0.0000"]]), s)  # 紧接着的下一条
    assert 100.1 not in book.asks
    assert book.last_update_id == 102


def test_gap_raises_and_book_untouched():
    book = new_book()
    s = stats()
    with pytest.raises(GapDetected):
        book.apply(ev(105, 106, b=[["100.0", "9.0000"]]), s)  # U=105 > L+1=101
    assert book.bids[100.0] == 1.0
    assert book.last_update_id == 100


def test_zero_quantity_deletes_level():
    book = new_book()
    s = stats()
    book.apply(ev(101, 101, b=[["99.9", "0.0000"]]), s)
    assert 99.9 not in book.bids
    assert book.bids[100.0] == 1.0


def test_quantity_is_absolute_not_delta():
    book = new_book()
    s = stats()
    book.apply(ev(101, 101, b=[["100.0", "5.0000"]]), s)
    book.apply(ev(102, 102, b=[["100.0", "7.0000"]]), s)
    assert book.bids[100.0] == 7.0  # 最新总量，而非累加


def test_trim_to_100_levels_per_side():
    snap = {
        "lastUpdateId": 1,
        "bids": [[f"{200.0 - i * 0.1:.1f}", "1.0000"] for i in range(150)],
        "asks": [[f"{200.1 + i * 0.1:.1f}", "1.0000"] for i in range(150)],
    }
    book = OrderBook()
    book.load_snapshot(snap)
    assert len(book.bids) == 100 and len(book.asks) == 100
    assert max(book.bids) == 200.0          # 保留最优 100 档
    assert min(book.asks) == pytest.approx(200.1)


# ------------------------- 清理顺序与关闭握手超时（修复回归测试） -------------------------


@pytest.fixture(autouse=True)
def reset_state():
    state.running = False
    state.status = "stopped"
    state.book = None
    state.generation = 0
    state.task = None
    state.last_error = None
    for k in state.stats:
        state.stats[k] = 0
    yield


class FakeTransport:
    def __init__(self):
        self.aborted = False

    def abort(self):
        self.aborted = True


class FakeWS:
    """模拟 WS 连接：按剧本发增量；close() 记录当时状态后卡住（关闭握手不响应）。"""

    def __init__(self, events, server_close=False, stall_close=True):
        self.queue = asyncio.Queue()
        for e in events:
            self.queue.put_nowait(json.dumps(e))
        self.server_close = server_close  # True: 发完剧本即结束接收（模拟断线）
        self.stall_close = stall_close    # True: close() 永不完成（关闭握手不响应）
        self.transport = FakeTransport()
        self.book_at_close = "未调用"      # close() 被调用时采集端对外发布的盘口
        self.error_at_close = None
        self.close_calls = 0

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        while True:
            if self.server_close and self.queue.empty():
                return  # 接收任务结束
            yield await self.queue.get()

    async def close(self):
        self.close_calls += 1
        self.book_at_close = state.book    # 记录清理开始时的对外发布状态
        self.error_at_close = state.last_error
        if self.stall_close:
            await asyncio.Future()         # 关闭握手永不完成


def make_snapshot():
    return {
        "lastUpdateId": 100,
        "bids": [["100.0", "1.0000"]],
        "asks": [["100.1", "1.5000"]],
    }


def patch_connect(monkeypatch, ws):
    calls = []

    async def fake_connect(*args, **kwargs):
        calls.append((args, kwargs))
        return ws

    monkeypatch.setattr(collector.websockets, "connect", fake_connect)
    return calls


def patch_snapshot(monkeypatch):
    async def fake_fetch():
        state.stats["snapshotAttempts"] += 1
        state.stats["snapshotOk"] += 1
        return make_snapshot()

    monkeypatch.setattr(collector, "fetch_snapshot", fake_fetch)


def test_close_connection_forces_abort_after_timeout(monkeypatch):
    """关闭握手不响应：最多等 CLOSE_TIMEOUT，随后强制断开底层传输。"""
    monkeypatch.setattr(collector, "CLOSE_TIMEOUT", 0.3)
    ws = FakeWS(events=[])

    async def run():
        start = time.monotonic()
        await collector.close_connection(ws)
        return time.monotonic() - start

    elapsed = asyncio.run(run())
    assert ws.transport.aborted            # 超时强制断开
    assert 0.25 <= elapsed < 1.0           # 总等待不超过 CLOSE_TIMEOUT（0.3s）


def test_gap_unpublishes_before_stalled_close(monkeypatch):
    """缺号：先撤下报价与编号、展示故障原因，再清理连接（关闭握手卡住也如此）。"""
    monkeypatch.setattr(collector, "CLOSE_TIMEOUT", 0.3)
    events = [ev(101, 101, b=[["100.0", "2.0000"]]), ev(105, 105)]  # 第二条缺号
    ws = FakeWS(events)
    patch_connect(monkeypatch, ws)
    patch_snapshot(monkeypatch)
    state.running = True

    async def run():
        start = time.monotonic()
        with pytest.raises(GapDetected):
            await collector.sync_cycle()
        return time.monotonic() - start

    elapsed = asyncio.run(run())
    assert ws.book_at_close is None                 # 清理开始前报价已撤下
    assert "缺号" in (ws.error_at_close or "")      # 故障原因已展示
    assert state.book is None
    assert state.status == "resyncing"
    assert state.stats["applied"] == 1              # 第一条增量已应用
    assert ws.transport.aborted                     # 卡住的握手被强制断开
    assert elapsed < 1.0                            # 清理总时长受 CLOSE_TIMEOUT 约束


def test_receive_end_unpublishes_before_close(monkeypatch):
    """接收任务结束（断线）：同样先撤下报价再清理连接。"""
    monkeypatch.setattr(collector, "CLOSE_TIMEOUT", 0.3)
    events = [ev(101, 101, b=[["100.0", "2.0000"]])]
    ws = FakeWS(events, server_close=True)  # 发完即结束接收
    patch_connect(monkeypatch, ws)
    patch_snapshot(monkeypatch)
    state.running = True

    async def run():
        with pytest.raises(collector.ResyncRequired):
            await collector.sync_cycle()

    asyncio.run(run())
    assert ws.book_at_close is None
    assert "连接问题" in (ws.error_at_close or "")
    assert state.status == "resyncing"


def test_stop_during_cleanup_does_not_reconnect(monkeypatch):
    """清理期间停采：清理照常完成（受 1 秒约束），但不得再发起新连接。"""
    monkeypatch.setattr(collector, "CLOSE_TIMEOUT", 0.3)
    events = [ev(101, 101), ev(105, 105)]  # 缺号触发清理
    ws = FakeWS(events)
    calls = patch_connect(monkeypatch, ws)
    patch_snapshot(monkeypatch)

    async def run():
        await collector.start_collection()
        # 等待进入清理（close 被调用，握手卡住）
        for _ in range(100):
            if ws.close_calls:
                break
            await asyncio.sleep(0.01)
        assert ws.close_calls == 1
        await collector.stop_collection()   # 清理期间停采
        await asyncio.sleep(0.5)            # 留时间给潜在的重连

    asyncio.run(run())
    assert len(calls) == 1                  # 没有重连
    assert state.status == "stopped"
    assert state.book is None
    assert ws.transport.aborted             # 卡住的握手仍被强制断开
