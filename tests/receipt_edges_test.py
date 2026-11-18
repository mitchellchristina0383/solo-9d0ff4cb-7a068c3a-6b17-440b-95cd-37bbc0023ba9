#!/usr/bin/env python3
"""回执路径的确定性边界测试（进程内 ASGI，不依赖 docker / uvicorn）。

self_test.py 覆盖真实网络下的并发、断开、重启；这里用注入故障的方式确定性地
覆盖三条难以在真实环境里稳定复现的路径：

  A. 完成回执写入失败 -> 不得交付上游成功结果（返回 500），在途计数仍归零；
  B. 回执登记失败 -> 直接拒绝转发（503），不产生无回执的在途请求；
  C. 客户端在处理过程中断开 -> 终态 client_disconnected，绝不写 upstream_ok。

用法：
    python3 tests/receipt_edges_test.py
依赖 fastapi/httpx（可用 PYLIBS 指定依赖目录，默认 /tmp/pylibs）。
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile

PYLIBS = os.environ.get("PYLIBS", "/tmp/pylibs")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path[:0] = [PYLIBS, os.path.join(ROOT, "gateway")]

_TMP = tempfile.mkdtemp(prefix="gw-edge-")
os.environ.setdefault("RECEIPTS_DB_PATH", os.path.join(_TMP, "receipts.sqlite3"))
os.environ.setdefault("UPSTREAMS", "node1=http://127.0.0.1:1")
os.environ.setdefault("PROBE_INTERVAL", "60")
os.environ.setdefault("REQUEST_TIMEOUT", "15")

import httpx  # noqa: E402
from starlette.requests import Request  # noqa: E402

import app as gw_app  # noqa: E402

passed = 0
failed = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global passed, failed
    if cond:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} {detail}")


def make_request(path: str = "slow", query: str = "seconds=99", disconnect_after: int | None = None) -> Request:
    """构造一个可控制断开时机的 Request。"""
    calls = 0

    async def receive():
        nonlocal calls
        calls += 1
        if disconnect_after is not None and calls > disconnect_after:
            return {"type": "http.disconnect"}
        return {"type": "http.request", "body": b"", "more_body": False}

    scope = {
        "type": "http",
        "method": "GET",
        "path": f"/proxy/{path}",
        "raw_path": f"/proxy/{path}".encode(),
        "query_string": query.encode(),
        "headers": [],
        "server": ("test", 80),
        "client": ("test", 1234),
        "scheme": "http",
        "root_path": "",
    }
    return Request(scope, receive)


class FakeUpstream:
    """替代 gateway.client：可控延迟后返回固定响应。"""

    def __init__(self, delay: float = 0.0, status: int = 200, body: bytes = b'{"ok":true}'):
        self.delay = delay
        self.status = status
        self.body = body
        self.closed_upstream = False

    async def request(self, method, url, params=None, timeout=None):
        await asyncio.sleep(self.delay)
        return httpx.Response(
            self.status,
            content=self.body,
            headers={"content-type": "application/json"},
        )


def prepare_node() -> None:
    node = gw_app.gateway.nodes[0]
    node.healthy = True
    node.draining = False
    node.inflight = 0
    gw_app.gateway.client = FakeUpstream()


async def test_a_complete_write_failure() -> None:
    print("== A. 完成回执写入失败不得交付成功 ==")
    prepare_node()
    original = gw_app.receipts.complete

    def boom(*args, **kwargs):
        # 只让「写成功回执」失败，便于观察网关如何补写失败终态
        if kwargs.get("outcome") == gw_app.OUTCOME_UPSTREAM_OK:
            raise gw_app.ReceiptStoreError("injected complete failure")
        return original(*args, **kwargs)

    gw_app.receipts.complete = boom  # type: ignore[assignment]
    try:
        resp = await gw_app.proxy("echo", make_request("echo", "a=1"))
    finally:
        gw_app.receipts.complete = original  # type: ignore[assignment]

    check("返回 500 而非上游 200", getattr(resp, "status_code", None) == 500, str(resp))
    check("错误体说明回执写入失败",
          b"receipt_write_failed" in resp.body, str(resp.body))
    check("在途计数已归零", gw_app.gateway.nodes[0].inflight == 0,
          str(gw_app.gateway.nodes[0].inflight))

    rows = gw_app.receipts.recent(5)
    latest = [r for r in rows if r["path"] == "echo"]
    check("失败的回执被补写为 gateway_failed（不是 upstream_ok）",
          bool(latest) and latest[0]["outcome"] == "gateway_failed"
          and latest[0]["confirmed"] is False, str(latest[:2]))

    # 若补写也失败，记录保持 in_flight，重启时必须标为未确认
    def always_boom(*args, **kwargs):
        raise gw_app.ReceiptStoreError("injected total failure")

    prepare_node()
    gw_app.receipts.complete = always_boom  # type: ignore[assignment]
    try:
        resp2 = await gw_app.proxy("echo", make_request("echo", "b=2"))
    finally:
        gw_app.receipts.complete = original  # type: ignore[assignment]
    check("补写也失败时仍返回 500 不交付上游结果",
          getattr(resp2, "status_code", None) == 500, str(resp2))

    store2 = gw_app.ReceiptStore(os.environ["RECEIPTS_DB_PATH"])
    repaired = [r for r in store2.recent(10) if r["outcome"] == "unconfirmed_after_restart"]
    check("重启后遗留 in_flight 标记为 unconfirmed_after_restart",
          bool(repaired) and all(r["confirmed"] is False for r in repaired), str(repaired[:2]))


async def test_b_begin_failure() -> None:
    print("== B. 回执登记失败拒绝转发 ==")
    prepare_node()
    original = gw_app.receipts.begin

    def boom(*args, **kwargs):
        raise gw_app.ReceiptStoreError("injected begin failure")

    gw_app.receipts.begin = boom  # type: ignore[assignment]
    try:
        resp = await gw_app.proxy("echo", make_request("echo", "a=1"))
    finally:
        gw_app.receipts.begin = original  # type: ignore[assignment]

    check("登记失败返回 503", getattr(resp, "status_code", None) == 503, str(resp))
    check("错误体说明回执库不可用",
          b"receipt_store_unavailable" in resp.body, str(resp.body))
    check("拒绝时没有产生在途计数", gw_app.gateway.nodes[0].inflight == 0,
          str(gw_app.gateway.nodes[0].inflight))


async def test_c_client_disconnect() -> None:
    print("== C. 客户端断开不写成功 ==")
    node = gw_app.gateway.nodes[0]
    node.healthy = True
    node.draining = False
    node.inflight = 0
    gw_app.gateway.client = FakeUpstream(delay=2.0)

    before = {r["receipt_id"] for r in gw_app.receipts.recent(200)}
    resp = await gw_app.proxy("slow", make_request("slow", "seconds=99", disconnect_after=2))
    check("客户端断开返回 499", getattr(resp, "status_code", None) == 499, str(resp))
    check("断开后在途计数归零", node.inflight == 0, str(node.inflight))

    new_rows = [r for r in gw_app.receipts.recent(200) if r["receipt_id"] not in before]
    check("新增回执恰好一条", len(new_rows) == 1, str(new_rows))
    if new_rows:
        r = new_rows[0]
        check("断开回执终态 client_disconnected",
              r["state"] == "completed" and r["outcome"] == "client_disconnected", str(r))
        check("断开回执不算成功且标记断开",
              r["confirmed"] is False and r["client_disconnected"] is True, str(r))
        check("断开回执不含成功上游结果", r["upstream_status"] is None, str(r))


async def main() -> int:
    await test_a_complete_write_failure()
    await test_b_begin_failure()
    await test_c_client_disconnect()
    print(f"\n==== 结果: {passed} passed, {failed} failed ====")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
