"""HTTP 入口网关（FastAPI + httpx），带持久转发回执。

职责：
  * 仅转发 GET 请求到上游，按轮询（round-robin）选择「健康且未排空」的节点；
  * 周期性主动探测上游，探测结论只影响健康标志，绝不改动人工排空标志；
  * 提供排空 / 恢复 / 状态管理 API；所有节点都不可接流时返回 503；
  * 排空期间新请求不再进入该节点，已在途的请求继续等待原响应返回；
  * 排空 API 阻塞到在途请求全部结束后才报告 drained=true；
  * 超时 / 连接错误 / 5xx 不虚报成功，并同步刷新健康状态，在途计数靠 try/finally 保证准确。

持久转发回执：
  * 每个 GET 在选路锁内先向 SQLite 登记唯一回执，再选节点并 inflight+1；
    三者同临界区，因此「有在途必有回执」「回执 → 节点 → 在途」时刻一致；
  * 能返回响应时，响应头 X-Receipt-Id 带回编号，可用 GET /receipts/{id} 查询
    选中节点、网关响应状态与上游结果；
  * 无可接流节点也会登记一条带拒绝原因的回执（503 也带编号）；
  * 上游超时 / 连接失败 / 客户端提前断开 / 网关被取消时，只写未确认终态，绝不写成功；
  * 完成记录写入失败时拒绝交付上游成功结果，改为返回 500；
  * 启动时把上次进程遗留的 in_flight 回执统一标记为 unconfirmed_after_restart；
  * 每个请求使用独立 uuid4 主键，并发请求不会互相覆盖回执。
"""
from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

logger = logging.getLogger("gateway.receipts")

# ----------------------------- 配置 -----------------------------

UPSTREAMS_RAW = os.environ.get(
    "UPSTREAMS",
    "node1=http://upstream1:8000,node2=http://upstream2:8000",
)
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "2.0"))
PROBE_TIMEOUT = float(os.environ.get("PROBE_TIMEOUT", "1.5"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "15.0"))
DRAIN_DEFAULT_TIMEOUT = float(os.environ.get("DRAIN_DEFAULT_TIMEOUT", "30.0"))

# 回执库路径；docker compose 会把它挂到命名卷上，保证重启后仍可追溯
RECEIPTS_DB_PATH = os.environ.get("RECEIPTS_DB_PATH", "./receipts.sqlite3")
# 回执里保留的上游响应体最大字节数（0 表示不保存响应体）
RECEIPT_BODY_LIMIT = max(0, int(os.environ.get("RECEIPT_BODY_LIMIT", "4096")))
# 客户端断开检测轮询间隔（秒）
DISCONNECT_POLL_INTERVAL = max(0.01, float(os.environ.get("DISCONNECT_POLL_INTERVAL", "0.1")))

# 透传响应时需要剥离的逐跳（hop-by-hop）头
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "content-length",
    "content-encoding",
}

# 回执终态。只有 upstream_ok 表示「网关取得了完整上游结果并已写好回执」；
# client_disconnected 会保留已取得的节点/上游信息，但 confirmed 恒为 false。
OUTCOME_IN_FLIGHT = "in_flight"
OUTCOME_UPSTREAM_OK = "upstream_ok"
OUTCOME_UPSTREAM_ERROR = "upstream_error"
OUTCOME_NO_UPSTREAM = "no_eligible_upstream"
OUTCOME_UPSTREAM_TIMEOUT = "upstream_timeout"
OUTCOME_UPSTREAM_UNREACHABLE = "upstream_unreachable"
OUTCOME_CLIENT_DISCONNECTED = "client_disconnected"
OUTCOME_FAILED = "gateway_failed"
OUTCOME_UNCONFIRMED_RESTART = "unconfirmed_after_restart"

_TERMINAL_OUTCOMES = {
    OUTCOME_UPSTREAM_OK,
    OUTCOME_UPSTREAM_ERROR,
    OUTCOME_NO_UPSTREAM,
    OUTCOME_UPSTREAM_TIMEOUT,
    OUTCOME_UPSTREAM_UNREACHABLE,
    OUTCOME_CLIENT_DISCONNECTED,
    OUTCOME_FAILED,
    OUTCOME_UNCONFIRMED_RESTART,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _parse_upstreams(raw: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not _:
            raise ValueError(f"非法 UPSTREAMS 条目: {item!r}，应为 name=url")
        out.append((name.strip(), url.strip().rstrip("/")))
    if not out:
        raise ValueError("至少需要配置一个上游节点")
    return out


# ----------------------------- 回执存储 -----------------------------

class ReceiptStoreError(RuntimeError):
    """回执库读写失败。"""


class ReceiptStore:
    """SQLite 回执库。

    每次操作使用独立连接 + 一把 threading.Lock 串行化。写库很轻量，且调用点
    都在事件循环线程；请求路径上的两次写入分别是「选路前的登记」与「交付前的
    完成记录」，后者在归还 inflight 之前完成，保证排空完成时回执已落终态。
    """

    def __init__(self, path: str) -> None:
        self.path = path
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        self._lock = threading.Lock()
        self._init_schema()
        # 启动即修复上一进程遗留的未结束回执；绝不把它们当作成功
        self.unconfirmed_open = self._mark_open_unconfirmed()

    # -- 连接与建表 --------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        try:
            conn = sqlite3.connect(self.path, timeout=10.0, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA busy_timeout=10000")
            return conn
        except sqlite3.Error as exc:
            raise ReceiptStoreError(
                f"无法连接回执库 {self.path!r}: {exc}"
            ) from exc

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            with self._lock, conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS receipts (
                        receipt_id          TEXT PRIMARY KEY,
                        gateway_pid         INTEGER NOT NULL,
                        created_at          TEXT NOT NULL,
                        completed_at        TEXT,
                        state               TEXT NOT NULL CHECK (state IN ('in_flight','completed')),
                        method              TEXT NOT NULL,
                        path                TEXT NOT NULL,
                        query               TEXT NOT NULL,
                        node                TEXT,
                        gw_status           INTEGER,
                        outcome             TEXT NOT NULL,
                        upstream_status     INTEGER,
                        upstream_body       TEXT,
                        upstream_body_len   INTEGER,
                        upstream_truncated  INTEGER,
                        detail              TEXT,
                        client_disconnected INTEGER NOT NULL DEFAULT 0
                    )
                    """
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_receipts_state_created "
                    "ON receipts(state, created_at)"
                )
        except sqlite3.Error as exc:
            raise ReceiptStoreError(f"无法初始化回执库 {self.path!r}: {exc}") from exc
        finally:
            conn.close()

    # -- 内部执行包装 -------------------------------------------------

    def _execute(self, sql: str, params: tuple = ()) -> int:
        conn = self._connect()
        try:
            with self._lock, conn:
                cur = conn.execute(sql, params)
                return cur.rowcount
        except sqlite3.Error as exc:
            raise ReceiptStoreError(f"回执库写入失败: {exc}") from exc
        finally:
            conn.close()

    def _query_one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        conn = self._connect()
        try:
            with self._lock, conn:
                return conn.execute(sql, params).fetchone()
        except sqlite3.Error as exc:
            raise ReceiptStoreError(f"回执库读取失败: {exc}") from exc
        finally:
            conn.close()

    # -- 生命周期操作 -------------------------------------------------

    def _mark_open_unconfirmed(self) -> int:
        conn = self._connect()
        try:
            with self._lock, conn:
                cur = conn.execute(
                    """
                    UPDATE receipts
                       SET state='completed',
                           outcome=?,
                           completed_at=?,
                           detail='网关进程重启，关闭前未记录到完成回执；上游结果未确认'
                     WHERE state='in_flight'
                    """,
                    (OUTCOME_UNCONFIRMED_RESTART, _now_iso()),
                )
                return cur.rowcount
        except sqlite3.Error as exc:
            raise ReceiptStoreError(f"无法修复遗留回执: {exc}") from exc
        finally:
            conn.close()

    def begin(self, method: str, path: str, query: str) -> tuple[str, str]:
        """登记一条 in_flight 回执，返回 (receipt_id, created_at)。"""
        receipt_id = uuid.uuid4().hex
        created_at = _now_iso()
        self._execute(
            """
            INSERT INTO receipts
                (receipt_id, gateway_pid, created_at, state, method, path, query, outcome)
            VALUES (?, ?, ?, 'in_flight', ?, ?, ?, ?)
            """,
            (receipt_id, os.getpid(), created_at, method, path, query, OUTCOME_IN_FLIGHT),
        )
        return receipt_id, created_at

    def complete(
        self,
        receipt_id: str,
        *,
        node: Optional[str],
        gw_status: Optional[int],
        outcome: str,
        upstream_status: Optional[int] = None,
        upstream_body: Optional[str] = None,
        upstream_body_len: Optional[int] = None,
        upstream_truncated: Optional[bool] = None,
        detail: Optional[str] = None,
        client_disconnected: bool = False,
    ) -> None:
        if outcome not in _TERMINAL_OUTCOMES:
            raise ValueError(f"非法终态 outcome: {outcome!r}")
        self._execute(
            """
            UPDATE receipts
               SET state='completed',
                   completed_at=?,
                   node=?,
                   gw_status=?,
                   outcome=?,
                   upstream_status=?,
                   upstream_body=?,
                   upstream_body_len=?,
                   upstream_truncated=?,
                   detail=?,
                   client_disconnected=?
             WHERE receipt_id=? AND state='in_flight'
            """,
            (
                _now_iso(),
                node,
                gw_status,
                outcome,
                upstream_status,
                upstream_body,
                upstream_body_len,
                None if upstream_truncated is None else int(upstream_truncated),
                detail,
                int(client_disconnected),
                receipt_id,
            ),
        )

    def get(self, receipt_id: str) -> Optional[dict]:
        row = self._query_one("SELECT * FROM receipts WHERE receipt_id=?", (receipt_id,))
        return None if row is None else _row_to_dict(row)

    def recent(self, limit: int) -> list[dict]:
        conn = self._connect()
        try:
            with self._lock, conn:
                rows = conn.execute(
                    "SELECT * FROM receipts ORDER BY created_at DESC, rowid DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        except sqlite3.Error as exc:
            raise ReceiptStoreError(f"回执库读取失败: {exc}") from exc
        finally:
            conn.close()
        return [_row_to_dict(r) for r in rows]


def _row_to_dict(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["upstream_truncated"] = (
        None if d.get("upstream_truncated") is None else bool(d["upstream_truncated"])
    )
    d["client_disconnected"] = bool(d.get("client_disconnected", 0))
    d["confirmed"] = d.get("state") == "completed" and d.get("outcome") == OUTCOME_UPSTREAM_OK
    return d


# ----------------------------- 节点状态 -----------------------------

@dataclass
class NodeState:
    name: str
    base_url: str
    # 健康标志：只由探测任务（以及请求失败时的降级）写入
    healthy: bool = False
    # 排空标志：只由人工 drain/resume 写入，探测永不触碰
    draining: bool = False
    inflight: int = 0
    rr_hits: int = 0
    success_count: int = 0
    failure_count: int = 0
    last_probe_at: Optional[float] = None
    last_probe_ok: Optional[bool] = None
    last_probe_detail: str = "尚未探测"
    last_change_at: float = field(default_factory=time.time)

    def can_serve(self) -> bool:
        return self.healthy and not self.draining

    def snapshot(self) -> dict:
        return {
            "name": self.name,
            "base_url": self.base_url,
            "healthy": self.healthy,
            "draining": self.draining,
            "eligible": self.can_serve(),
            "inflight": self.inflight,
            "rr_hits": self.rr_hits,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "last_probe_at": self.last_probe_at,
            "last_probe_ok": self.last_probe_ok,
            "last_probe_detail": self.last_probe_detail,
        }


class Gateway:
    def __init__(self, nodes: list[NodeState], store: ReceiptStore) -> None:
        self.nodes = nodes
        self._by_name = {n.name: n for n in nodes}
        self.receipts = store
        # 一把条件变量保护所有节点状态；请求结束 / 排空等待都在其上通知
        self.cond = asyncio.Condition()
        self._rr = 0
        self.client: Optional[httpx.AsyncClient] = None
        self._probe_task: Optional[asyncio.Task] = None

    def get(self, name: str) -> Optional[NodeState]:
        return self._by_name.get(name)

    async def pick_node(
        self, method: str, path: str, query: str
    ) -> tuple[str, Optional[NodeState]]:
        """同一临界区内完成：登记回执 -> 轮询选路 -> inflight+1。

        返回 (receipt_id, node)；无节点可接流时返回 (receipt_id, None)。
        登记失败抛 ReceiptStoreError，调用方不得继续转发（否则会产生无回执的在途）。
        """
        async with self.cond:
            receipt_id, _ = self.receipts.begin(method, path, query)
            n = len(self.nodes)
            for offset in range(n):
                idx = (self._rr + offset) % n
                node = self.nodes[idx]
                if node.can_serve():
                    self._rr = (idx + 1) % n
                    node.inflight += 1
                    node.rr_hits += 1
                    return receipt_id, node
            return receipt_id, None

    async def mark_unhealthy(self, node: NodeState, detail: str) -> None:
        """请求失败时的即时降级；只改健康标志，不动 draining。"""
        async with self.cond:
            node.healthy = False
            node.last_probe_ok = False
            node.last_probe_detail = f"请求失败降级: {detail}"
            node.last_probe_at = time.time()
            node.last_change_at = time.time()

    async def probe_one(self, node: NodeState) -> None:
        """对单个节点做主动探测。成功只置 healthy=True，绝不解除 draining。"""
        assert self.client is not None
        try:
            resp = await self.client.get(f"{node.base_url}/", timeout=PROBE_TIMEOUT)
            ok = resp.status_code < 500
            detail = f"HTTP {resp.status_code}"
        except httpx.TimeoutException as exc:
            ok, detail = False, f"探测超时: {type(exc).__name__}"
        except httpx.RequestError as exc:
            ok, detail = False, f"探测连接错误: {type(exc).__name__}: {str(exc)[:100]}"

        async with self.cond:
            node.healthy = ok
            # 注意：这里刻意不修改 node.draining
            node.last_probe_at = time.time()
            node.last_probe_ok = ok
            node.last_probe_detail = detail
            node.last_change_at = time.time()

    async def probe_all(self) -> None:
        await asyncio.gather(*(self.probe_one(n) for n in self.nodes))

    async def _probe_loop(self) -> None:
        while True:
            try:
                await self.probe_all()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # 探测循环本身不能崩
                print(f"[probe-loop] unexpected error: {exc!r}")
            await asyncio.sleep(PROBE_INTERVAL)

    async def drain(self, node: NodeState, timeout: Optional[float]) -> tuple[bool, int]:
        """置排空标志并等待在途清零。返回 (是否排空完成, 剩余在途数)。"""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + max(timeout, 0.0)
        async with self.cond:
            node.draining = True
            node.last_change_at = time.time()
            while node.inflight > 0:
                if deadline is None:
                    await self.cond.wait()
                    continue
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return False, node.inflight
                # asyncio.Condition.wait 不支持 timeout，用 wait_for 限时等待
                try:
                    await asyncio.wait_for(self.cond.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass  # 回到循环重新计算剩余时间 / 在途数
            return True, 0

    async def resume(self, node: NodeState) -> None:
        """恢复接流：先清排空并保守置为不健康，再立即探测，由探测决定能否接流。"""
        async with self.cond:
            node.draining = False
            # 恢复不绕过故障探测：探测结论出来前一律不接流，杜绝状态窗口
            node.healthy = False
            node.last_probe_ok = None
            node.last_probe_detail = "恢复接流，等待首次探测确认"
            node.last_change_at = time.time()
        await self.probe_one(node)


# ----------------------------- 应用生命周期 -----------------------------

receipts = ReceiptStore(RECEIPTS_DB_PATH)
gateway = Gateway(
    [NodeState(name, url) for name, url in _parse_upstreams(UPSTREAMS_RAW)],
    receipts,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with httpx.AsyncClient() as client:
        gateway.client = client
        # 启动先做一轮探测，避免把流量打到未知状态的节点
        await gateway.probe_all()
        gateway._probe_task = asyncio.create_task(gateway._probe_loop())
        try:
            yield
        finally:
            if gateway._probe_task:
                gateway._probe_task.cancel()
                try:
                    await gateway._probe_task
                except asyncio.CancelledError:
                    pass


app = FastAPI(
    title="http-entry-gateway",
    version="2.0.0",
    description=(
        "仅转发 GET 的入口网关：轮询健康且未排空的上游，支持排空/恢复/状态管理，"
        "无可接流节点时返回 503；每次转发落 SQLite 回执，可用编号追溯节点与上游结果。"
    ),
    lifespan=lifespan,
)


# ----------------------------- 转发接口 -----------------------------

@app.get("/", tags=["meta"])
async def root() -> dict:
    return {
        "service": "http-entry-gateway",
        "proxy": "GET /proxy/{path} 仅支持 GET；响应头 X-Receipt-Id 为转发回执编号",
        "receipts": ["GET /receipts/{receipt_id}", "GET /receipts?limit=20"],
        "admin": [
            "GET  /admin/status",
            "POST /admin/nodes/{name}/drain?timeout=",
            "POST /admin/nodes/{name}/resume",
            "POST /admin/probe",
        ],
    }


async def _wait_upstream_or_disconnect(awaitable, request: Request):
    """等待上游动作，同时轮询客户端断开。

    返回 (True, 结果) 或 (False, None)。客户端先断开时取消上游等待，
    避免把未确认结果写成成功；不能让上游任务在后台继续跑。
    """
    task = asyncio.ensure_future(awaitable)
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=DISCONNECT_POLL_INTERVAL)
            if done:
                return True, task.result()
            if await request.is_disconnected():
                task.cancel()
                with suppress(BaseException):
                    await task
                return False, None
    finally:
        if not task.done():
            task.cancel()
            with suppress(BaseException):
                await task


def _body_preview(body: bytes, truncated: bool) -> str:
    """回执里的响应体预览；限长截断，非 UTF-8 只记长度占位。"""
    chunk = body[:RECEIPT_BODY_LIMIT] if RECEIPT_BODY_LIMIT else b""
    if not chunk and body:
        return f"<omitted {len(body)} bytes>"
    try:
        text = chunk.decode("utf-8")
    except UnicodeDecodeError:
        return f"<binary {len(body)} bytes>"
    return text + ("…(truncated)" if truncated else "")


@app.get("/proxy/{path:path}", tags=["proxy"])
async def proxy(path: str, request: Request) -> Response:
    # 1) 回执登记 + 选路 + inflight+1（同一临界区）
    try:
        receipt_id, node = await gateway.pick_node("GET", path, request.url.query)
    except ReceiptStoreError as exc:
        logger.exception("登记回执失败，拒绝转发")
        return JSONResponse(
            status_code=503,
            content={
                "error": "receipt_store_unavailable",
                "detail": f"回执登记失败，未转发: {exc}",
            },
        )

    # 2) 无节点可接流：保留拒绝原因回执，503 也带编号
    if node is None:
        try:
            receipts.complete(
                receipt_id,
                node=None,
                gw_status=503,
                outcome=OUTCOME_NO_UPSTREAM,
                detail="当前没有健康且未排空的上游节点可接流",
            )
        except ReceiptStoreError:
            # 拒绝原因没写进去也不能装作成功（本来就是失败），照常返回 503
            logger.exception("写拒绝回执失败 receipt=%s", receipt_id)
        return JSONResponse(
            status_code=503,
            headers={"X-Receipt-Id": receipt_id},
            content={
                "error": "no_eligible_upstream",
                "detail": "当前没有健康且未排空的上游节点可接流",
                "receipt_id": receipt_id,
            },
        )

    released = False
    recorded = False

    async def finish(
        *,
        ok: bool,
        outcome: str,
        gw_status: int,
        upstream_status: Optional[int] = None,
        body: Optional[bytes] = None,
        body_truncated: bool = False,
        detail: Optional[str] = None,
        client_disconnected: bool = False,
    ) -> None:
        """写完成回执 -> 归还在途计数（顺序固定）。

        回执写入失败时 ReceiptStoreError 会穿过本函数，但 finally 仍会归还计数，
        避免在途泄漏；调用方据此拒绝交付成功结果。
        """
        nonlocal released, recorded
        try:
            if not recorded:
                recorded = True
                preview = None
                body_len = None
                if body is not None:
                    preview = _body_preview(body, body_truncated)
                    body_len = len(body)
                receipts.complete(
                    receipt_id,
                    node=node.name,
                    gw_status=gw_status,
                    outcome=outcome,
                    upstream_status=upstream_status,
                    upstream_body=preview,
                    upstream_body_len=body_len,
                    upstream_truncated=body_truncated if body is not None else None,
                    detail=detail,
                    client_disconnected=client_disconnected,
                )
        finally:
            if not released:
                released = True

                async def do_release() -> None:
                    async with gateway.cond:
                        node.inflight -= 1
                        if ok:
                            node.success_count += 1
                        else:
                            node.failure_count += 1
                        # 在途归零可能让 drain 等待者满足条件
                        gateway.cond.notify_all()

                # shield 保护计数归还：即便外层任务此刻被取消，也要把 inflight
                # 减回去并唤醒 drain 等待者，不能让在途计数泄漏。
                release_task = asyncio.ensure_future(do_release())
                try:
                    await asyncio.shield(release_task)
                except asyncio.CancelledError:
                    # 等归还真正完成后再让取消继续向上传播
                    with suppress(BaseException):
                        await asyncio.shield(release_task)
                    raise

    async def finish_best_effort(**kwargs) -> None:
        """取消/异常兜底：任何一步失败都只记日志，绝不二次归还计数。"""
        try:
            await finish(**kwargs)
        except BaseException:
            logger.exception("兜底写回执失败 receipt=%s", receipt_id)

    target_url = f"{node.base_url}/{path}"
    try:
        try:
            finished, upstream_resp = await _wait_upstream_or_disconnect(
                gateway.client.request(
                    "GET",
                    target_url,
                    params=request.query_params,
                    timeout=REQUEST_TIMEOUT,
                ),
                request,
            )
        except httpx.TimeoutException as exc:
            await gateway.mark_unhealthy(node, f"请求超时 {type(exc).__name__}")
            await finish(
                ok=False,
                outcome=OUTCOME_UPSTREAM_TIMEOUT,
                gw_status=504,
                detail=f"上游超时 {type(exc).__name__}；上游结果未确认",
            )
            return JSONResponse(
                status_code=504,
                headers={"X-Receipt-Id": receipt_id},
                content={
                    "error": "upstream_timeout",
                    "node": node.name,
                    "receipt_id": receipt_id,
                },
            )
        except httpx.RequestError as exc:
            await gateway.mark_unhealthy(node, f"{type(exc).__name__}: {str(exc)[:100]}")
            await finish(
                ok=False,
                outcome=OUTCOME_UPSTREAM_UNREACHABLE,
                gw_status=502,
                detail=f"上游不可达 {type(exc).__name__}: {str(exc)[:100]}；上游结果未确认",
            )
            return JSONResponse(
                status_code=502,
                headers={"X-Receipt-Id": receipt_id},
                content={
                    "error": "upstream_unreachable",
                    "node": node.name,
                    "receipt_id": receipt_id,
                },
            )

        if not finished:
            # 客户端在拿到上游响应前断开：上游结果未确认，绝不写成功
            await finish(
                ok=False,
                outcome=OUTCOME_CLIENT_DISCONNECTED,
                gw_status=499,
                detail="客户端在网关取得上游响应前断开；上游结果未确认",
                client_disconnected=True,
            )
            return Response(status_code=499, headers={"X-Receipt-Id": receipt_id})

        # 3) 读取响应体：同样监听客户端断开
        try:
            body_finished, body = await _wait_upstream_or_disconnect(
                upstream_resp.aread(), request
            )
        except (httpx.TimeoutException, httpx.RequestError) as exc:
            await gateway.mark_unhealthy(node, f"读取响应体失败 {type(exc).__name__}")
            await finish(
                ok=False,
                outcome=OUTCOME_UPSTREAM_ERROR,
                gw_status=502,
                upstream_status=upstream_resp.status_code,
                detail=f"读取上游响应体失败 {type(exc).__name__}；上游结果未确认",
            )
            return JSONResponse(
                status_code=502,
                headers={"X-Receipt-Id": receipt_id},
                content={
                    "error": "upstream_body_read_failed",
                    "node": node.name,
                    "receipt_id": receipt_id,
                },
            )

        if not body_finished:
            await finish(
                ok=False,
                outcome=OUTCOME_CLIENT_DISCONNECTED,
                gw_status=499,
                upstream_status=upstream_resp.status_code,
                detail="客户端在网关读完上游响应体前断开；结果未交付",
                client_disconnected=True,
            )
            return Response(status_code=499, headers={"X-Receipt-Id": receipt_id})

        upstream_status = upstream_resp.status_code
        upstream_ok = upstream_status < 500
        if not upstream_ok:
            await gateway.mark_unhealthy(node, f"上游返回 HTTP {upstream_status}")

        truncated = len(body) > RECEIPT_BODY_LIMIT

        # 交付前确认客户端仍在，避免把未交付的结果记成成功（这里只读一次断开状态）
        if await request.is_disconnected():
            await finish(
                ok=False,
                outcome=OUTCOME_CLIENT_DISCONNECTED,
                gw_status=499,
                upstream_status=upstream_status,
                body=body,
                body_truncated=truncated,
                detail=f"已取得上游 HTTP {upstream_status}，但客户端已断开；结果未交付",
                client_disconnected=True,
            )
            return Response(status_code=499, headers={"X-Receipt-Id": receipt_id})

        detail = (
            f"上游 HTTP {upstream_status}，响应 {len(body)} 字节"
            if upstream_ok
            else f"上游 HTTP {upstream_status}，失败结果如实返回"
        )

        # 完成回执写成功后才交付响应；写失败会抛 ReceiptStoreError 走 500
        await finish(
            ok=upstream_ok,
            outcome=OUTCOME_UPSTREAM_OK if upstream_ok else OUTCOME_UPSTREAM_ERROR,
            gw_status=upstream_status,
            upstream_status=upstream_status,
            body=body,
            body_truncated=truncated,
            detail=detail,
        )
        assert recorded

        headers = {
            k: v for k, v in upstream_resp.headers.items() if k.lower() not in _HOP_BY_HOP
        }
        headers["X-Gateway"] = "http-entry-gateway"
        headers["X-Upstream-Node"] = node.name
        headers["X-Receipt-Id"] = receipt_id
        return Response(
            content=body,
            status_code=upstream_status,
            headers=headers,
            media_type=upstream_resp.headers.get("content-type"),
        )

    except ReceiptStoreError:
        logger.exception("完成回执写入失败 receipt=%s", receipt_id)
        # 尽力补写一条失败终态，避免当前进程里留下永远 in_flight 的记录；
        # 再失败就只能等重启修复，但无论如何都不交付上游结果（成功也不行）。
        try:
            receipts.complete(
                receipt_id,
                node=node.name,
                gw_status=500,
                outcome=OUTCOME_FAILED,
                detail="完成回执写入失败；上游结果未交付",
            )
        except ReceiptStoreError:
            logger.exception("补写失败终态仍失败 receipt=%s", receipt_id)
        return JSONResponse(
            status_code=500,
            headers={"X-Receipt-Id": receipt_id},
            content={
                "error": "receipt_write_failed",
                "detail": "完成回执写入失败，未交付上游结果",
                "receipt_id": receipt_id,
            },
        )
    except asyncio.CancelledError:
        await finish_best_effort(
            ok=False,
            outcome=OUTCOME_FAILED,
            gw_status=499,
            detail="网关在转发过程中被取消；上游结果未确认",
        )
        raise
    except Exception:
        logger.exception("转发过程内部异常 receipt=%s", receipt_id)
        await finish_best_effort(
            ok=False,
            outcome=OUTCOME_FAILED,
            gw_status=500,
            detail="网关内部异常；上游结果未确认",
        )
        return JSONResponse(
            status_code=500,
            headers={"X-Receipt-Id": receipt_id},
            content={
                "error": "gateway_internal_error",
                "detail": "网关内部异常，未交付上游结果",
                "receipt_id": receipt_id,
            },
        )


# 非 GET 方法访问转发路径：FastAPI 自动返回 405 Method Not Allowed


# ----------------------------- 回执查询接口 -----------------------------

@app.get("/receipts/{receipt_id}", tags=["receipts"])
async def get_receipt(receipt_id: str):
    try:
        row = receipts.get(receipt_id)
    except ReceiptStoreError as exc:
        return JSONResponse(
            status_code=503,
            content={"error": "receipt_store_unavailable", "detail": str(exc)},
        )
    if row is None:
        return JSONResponse(
            status_code=404,
            content={"error": "receipt_not_found", "receipt_id": receipt_id},
        )
    return row


@app.get("/receipts", tags=["receipts"])
async def list_receipts(limit: int = 20):
    limit = max(1, min(limit, 200))
    try:
        rows = receipts.recent(limit)
    except ReceiptStoreError as exc:
        return JSONResponse(
            status_code=503,
            content={"error": "receipt_store_unavailable", "detail": str(exc)},
        )
    return {"count": len(rows), "items": rows}


# ----------------------------- 管理接口 -----------------------------

@app.get("/admin/status", tags=["admin"])
async def admin_status() -> dict:
    async with gateway.cond:
        return {
            "probe_interval": PROBE_INTERVAL,
            "request_timeout": REQUEST_TIMEOUT,
            "receipts_db": RECEIPTS_DB_PATH,
            "unconfirmed_open_at_startup": receipts.unconfirmed_open,
            "nodes": [n.snapshot() for n in gateway.nodes],
            "eligible_nodes": [n.name for n in gateway.nodes if n.can_serve()],
        }


@app.post("/admin/nodes/{name}/drain", tags=["admin"])
async def admin_drain(name: str, timeout: Optional[float] = DRAIN_DEFAULT_TIMEOUT):
    """排空节点：立即停止分配新请求，并阻塞等待在途请求全部结束。

    只有在途数归零才返回 drained=true；超时仍有在途请求时返回 202，
    drained=false 且节点继续保持排空状态。
    """
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    drained, inflight = await gateway.drain(node, timeout)
    body = {
        "node": name,
        "draining": True,
        "drained": drained,
        "inflight": inflight,
        "detail": "在途请求已全部结束" if drained else "等待在途请求结束超时，节点仍处于排空中",
    }
    return JSONResponse(status_code=200 if drained else 202, content=body)


@app.post("/admin/nodes/{name}/resume", tags=["admin"])
async def admin_resume(name: str):
    node = gateway.get(name)
    if node is None:
        return JSONResponse(status_code=404, content={"error": "unknown_node", "name": name})

    await gateway.resume(node)
    snap = node.snapshot()
    return {
        "node": name,
        "draining": False,
        "healthy": snap["healthy"],
        "eligible": snap["eligible"],
        "last_probe_detail": snap["last_probe_detail"],
        "detail": "已解除排空；是否可接流以健康探测结论为准",
    }


@app.post("/admin/probe", tags=["admin"])
async def admin_probe() -> dict:
    """手动触发一轮探测（探测不会改变任何节点的排空标志）。"""
    await gateway.probe_all()
    async with gateway.cond:
        return {"nodes": [n.snapshot() for n in gateway.nodes]}
