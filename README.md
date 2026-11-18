# HTTP 入口网关（优雅排空 / 健康探测 / 轮询转发 / 持久转发回执）

滚动更新场景下的入口网关：运维人员先排空待停机实例，使新请求不再进入，
同时保证在途长请求完整取回原响应，全部结束后才报告排空完成。
在此之上，每次 `GET /proxy/*` 都落一条 **SQLite 转发回执**：值班工程师凭客户端
响应头里的编号，就能查清这次请求进了哪台实例、网关自己返回了什么、上游给了什么结果。

技术栈：**FastAPI + httpx + 标准库 sqlite3**，由 **Docker Compose** 启动 1 个网关 +
2 个可模拟慢请求 / 故障的上游。

## 目录结构

```
.
├── docker-compose.yml          # 编排：gateway + upstream1 + upstream2（回执卷 receipts）
├── gateway/
│   ├── app.py                  # 网关：转发、探测、排空/恢复/状态、持久回执与查询
│   ├── requirements.txt
│   └── Dockerfile
├── upstream/
│   ├── app.py                  # 模拟上游：slow / flaky / 故障模式控制
│   ├── requirements.txt
│   └── Dockerfile
└── tests/
    ├── self_test.py            # 端到端自测（21 组场景，79 项断言，含并发/断开/重启）
    └── receipt_edges_test.py   # 回执边界单测（注入回执库故障，15 项断言）
```

## 一键启动

```bash
docker compose up --build
# 网关:        http://localhost:18080
# 上游 node1:  http://localhost:18081
# 上游 node2:  http://localhost:18082
# 交互文档:    http://localhost:18080/docs      ← 转发、回执查询、管理都在这里操作
```

网关环境变量（见 `docker-compose.yml`）：

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `UPSTREAMS` | `node1=http://upstream1:8000,node2=http://upstream2:8000` | 上游列表 `name=url` |
| `PROBE_INTERVAL` | `2.0` | 周期探测间隔（秒） |
| `PROBE_TIMEOUT` | `1.5` | 探测超时（秒） |
| `REQUEST_TIMEOUT` | `15.0` | 转发请求超时（秒） |
| `DRAIN_DEFAULT_TIMEOUT` | `30.0` | 排空 API 默认等待上限（秒） |
| `RECEIPTS_DB_PATH` | `./receipts.sqlite3`（compose 内为 `/data/receipts.sqlite3`） | 回执库路径，挂在命名卷 `receipts` 上 |
| `RECEIPT_BODY_LIMIT` | `4096` | 回执里保留的上游响应体最大字节数（0 表示不存体） |
| `DISCONNECT_POLL_INTERVAL` | `0.1` | 客户端断开检测轮询间隔（秒） |

## API 一览（均可在 /docs 直接操作）

转发（仅 GET）：

```
GET /proxy/{path}      # 轮询转发到健康且未排空的节点，透传查询串与响应
                       # 成功/失败响应都带 X-Receipt-Id 响应头
                       # 无可用节点 -> 503（也带编号，回执记录拒绝原因）
                       # 上游超时 -> 504；连不上 -> 502；回执不可信 -> 500
                       # 非 GET 方法 -> 405
```

回执查询（值班排查入口）：

```
GET /receipts/{receipt_id}   # 单条回执：节点 / 网关状态 / 上游结果 / 终态 / confirmed
GET /receipts?limit=20       # 最近回执列表（按创建时间倒序，limit 上限 200）
```

回执字段要点：

| 字段 | 含义 |
| --- | --- |
| `receipt_id` | uuid4，每次请求唯一；与响应头 `X-Receipt-Id` 一致 |
| `state` | `in_flight` / `completed` |
| `outcome` | `upstream_ok`、`upstream_error`、`no_eligible_upstream`、`upstream_timeout`、`upstream_unreachable`、`client_disconnected`、`gateway_failed`、`unconfirmed_after_restart` |
| `confirmed` | 仅当 `state=completed` 且 `outcome=upstream_ok` 时为 `true`，其余一律 `false` |
| `node` | 选中的上游节点（无节点可接流或登记后未选路时为空） |
| `gw_status` / `upstream_status` | 网关返回给客户端的状态码 / 上游自身状态码（后者未取得时为空） |
| `upstream_body`、`upstream_body_len`、`upstream_truncated` | 上游结果预览（限长）与真实长度 |
| `detail` | 人类可读的结论 / 拒绝原因 |
| `client_disconnected` | 客户端是否在结果交付前断开 |

管理（保留原有语义）：

```
GET  /admin/status                        # 节点健康/排空/在途/计数/最近探测；回执库路径与启动修复数
POST /admin/nodes/{name}/drain?timeout=#   # 排空：立即停接新流，阻塞到在途清零
                                          #   在途归零 -> 200 drained=true
                                          #   超时仍有在途 -> 202 drained=false
POST /admin/nodes/{name}/resume           # 恢复：清排空，立即探测，由探测决定能否接流
POST /admin/probe                         # 手动触发一轮探测
```

上游模拟控制（演练用，直连上游端口，不经过网关）：

```
GET  /control/state            # 查看模式与在途数
POST /control/mode?mode=       # healthy | slow | error | hang
GET  /slow?seconds=N           # 慢请求（验证在途不被截断）
GET  /flaky                    # 按模式返回 200 / 500 / 挂起
GET  /                         # 健康端点（error->503, hang->挂起）
```

## 典型演练

```bash
# 1. 转发 + 用回执编号追溯
curl -i "http://localhost:18080/proxy/echo?tag=demo"
# 响应头 X-Receipt-Id: 3f0c...；然后：
curl http://localhost:18080/receipts/3f0c...

# 2. 发一个慢请求（10 秒），在另一终端立刻排空 node2
curl "http://localhost:18080/proxy/slow?seconds=10" &
curl -X POST "http://localhost:18080/admin/nodes/node2/drain?timeout=30"
# 现象：drain 请求阻塞，约 10 秒后才返回 drained=true；慢请求拿到完整 200

# 3. 让 node1 进入故障模式，观察探测自动摘除
curl -X POST "http://localhost:18081/control/mode?mode=error"
sleep 3
curl http://localhost:18080/admin/status      # node1 healthy=false

# 4. 故障未消除时恢复 node1：不会绕过探测，节点仍不可接流
curl -X POST http://localhost:18080/admin/nodes/node1/resume
# eligible=false；上游修好后经周期探测自动恢复接流
```

## 关键设计与并发正确性

- **回执登记、选路、在途计数同一临界区**：`GET /proxy/*` 在 `asyncio.Condition`
  的一次临界区内完成「写 in_flight 回执 → 轮询选节点 → inflight+1」。
  因此有在途请求必有回执，回执里的节点就是实际接流的节点，计数与回执不会错位。
- **完成回执先落库、再交付**：`finish()` 固定先写完成回执，再（在 `finally` 中）
  归还 inflight 并唤醒排空等待者；所以 drain 报 `drained=true` 时，对应的回执
  已是终态。完成记录写入失败会 `ReceiptStoreError`，网关**不交付上游结果**，
  改为返回 500 `receipt_write_failed`（尽力补写一条 `gateway_failed` 终态）。
- **两种独立的「未确认」**：没有取得上游响应（超时/连接失败/取消/客户端先断开）
  时只写未确认终态，绝不写 `upstream_ok`；客户端在结果交付前断开同样记为
  `client_disconnected` 且 `confirmed=false`。全部响应都带编号，失败也查得到原因。
- **重启修复**：进程启动时先把所有残留 `in_flight` 回执批量标成
  `unconfirmed_after_restart`（`/admin/status` 的 `unconfirmed_open_at_startup`
  报告修复条数），绝不把崩溃前的在途请求当成成功。
- **并发不互相覆盖**：每个请求独立 `uuid4` 主键，完成记录是带
  `WHERE receipt_id=? AND state='in_flight'` 的单行更新；自测用 12 个并发请求
  逐条核对编号与查询串一一对应。
- **两个独立标志位**：`healthy` 只由探测（含请求失败降级）写入；`draining` 只由
  人工 drain/resume 写入。因此**探测成功永不解除排空**，两者交错互不串改。
- **恢复不绕过故障探测**：`resume` 先清排空并保守置 `healthy=false`，再做一次
  真实探测；探测通过前该节点不会被轮询选中。
- **在途计数可靠**：转发用 `try/finally` 归还计数，成功、5xx、超时、连接错误、
  客户端取消都会准确递减；归还本身用 `asyncio.shield` 保护，取消途中也不会漏减；
  排空等待在条件变量上，请求一结束即被唤醒。
- **排空语义**：置位后新请求立即绕过该节点；已在途请求不被取消，继续等待并
  透传原响应；只有 `inflight` 归零才返回 `drained=true`。
- **不虚报成功**：5xx 如实透传并即时降级；超时返回 504、连接错误返回 502，
  均标记失败并刷新健康状态；回执不可信时返回 500，绝不返回虚假 200。
- **无节点可接流**：所有节点都不健康或排空中时，`GET /proxy/*` 返回 503，
  同时留下 `no_eligible_upstream` 回执。

## 自测

```bash
# 端到端（无需 docker；需本地具备 fastapi/uvicorn/httpx，可 pip install 到独立目录）
PYLIBS=/tmp/pylibs python3 tests/self_test.py
# 21 组场景 / 79 项断言：原有 15 组（轮询、排空、探测交错、503/504/502、405、/docs …）
# 加上 16-21：成功回执、12 路并发回执不覆盖、503 拒绝回执、超时回执未确认、
# 客户端断开回执未确认、SIGKILL 重启后未结束回执标记 unconfirmed_after_restart。

# 回执边界（进程内注入故障，确定性）
PYLIBS=/tmp/pylibs python3 tests/receipt_edges_test.py
# 15 项断言：完成回执写失败->500 且不交付上游结果、登记失败->503 且无在途、
# 客户端断开->client_disconnected 且 confirmed=false。
```

## 已知限制与遗留问题

- **单进程 / 单实例假设**：回执、节点状态、在途计数都在进程内存里，网关只能
  单 worker 运行（`uvicorn` 未开 `--workers`）。多副本需要把选路与计数迁移到
  共享存储/协调服务，当前设计不支持。
- **回执库写入在事件循环线程内同步执行**：单次写入很小（毫秒级），目前够用；
  若回执量级继续上升，应改为专用线程或 `aiosqlite`，避免阻塞事件循环。
- **客户端断开检测是主动轮询**（默认 100 ms）：断开到写终态之间存在一个轮询
  窗口；断开前已完整取得上游结果、但尚未写回执的极窄窗口仍会记成
  `upstream_ok`（该结果已完整取得，仅副作用是无法确认客户端是否收到）。
- **响应体预览限长**：`RECEIPT_BODY_LIMIT` 之外只记长度与 `truncated`，且只按
  UTF-8 解码，二进制响应仅记录占位说明。
- **回执无保留期清理**：表会持续增长，生产中需要按 `created_at` 做定期归档/删除。
- **未做压力测试**：并发仅验证了 12 路正确性，未测吞吐与长稳。
