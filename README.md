# 断点续传文件 API（Resumable Upload API）

面向弱网客户端的分片上传/断点续传服务：客户端先创建上传会话，再按**任意顺序**提交带起止偏移的二进制分片；服务端拒绝越界或与已收区间重叠的分片，并通过状态接口返回缺失区间。仅当分片**完整覆盖整个文件**且整文件 **SHA-256 校验通过**时才原子完成并开放下载——任何失败都不会产生可下载成品。

创建会话支持**幂等键**：`POST /sessions` 可携带 `Idempotency-Key` 头，首次响应丢失或并发重试时返回同一会话，不会重复建会话（详见下文「创建上传会话」）。

已经完整上传并校验通过的会话还可以作为**复制来源**：`POST /sessions/{id}/copy-chunk` 由服务端直接把来源成品文件的指定字节区间复制为目标会话的一个分片，弱网客户端无需重新上传重复内容（详见下文「服务端复制分片」）。

已经登记的分片如果事后发现内容损坏（例如最后整文件 SHA-256 校验失败），不必放弃整个会话：`PUT /sessions/{id}/chunks/repair` 可对**一条已登记分片原位替换为等长新内容**，缺失区间与已收字节数保持不变，修复后重新完成整文件校验即可（详见下文「已收分片原位修复」）。

定位损坏分片也不用客户端盲猜：`POST /sessions/{id}/chunks/audit` 接收客户端的分片摘要清单（最多 32 条），**按起点顺序流式核对磁盘分片**，逐条返回匹配、摘要不符、文件缺失或长度不符及实际摘要，直接圈定 `repair` 的目标区间；诊断全程只读、持有会话锁，不会与修复/完成/放弃交错产生误报（详见下文「分片完整性诊断」）。

多个上传者协作同一会话时，可以用**分片认领**避免重复上传：`POST /sessions/{id}/chunk-claims` 认领一个尚未接收的半开区间，获得一个 **5 分钟有效**的一次性随机令牌；认领有效期间，与该区间相交的分片只有凭 `X-Chunk-Claim` 头提交**令牌匹配且区间完全一致**的请求才会被接受，成功提交即消费令牌，过期后可重新认领（详见下文「分片认领」）。

多端协作断线重连时，不必各自全量重查状态：`GET /sessions/{id}/progress` 在**同一只读读事务**中返回当前已收区间（基线）与会话事件游标，`GET /sessions/{id}/events?cursor=&limit=` 再按游标分页、按序返回其他客户端成功接收的区间（页大小 1～100）；普通上传与服务端复制分片成功登记时在**同一登记事务**内写入同会话单调递增的事件序号，失败、重试冲突与分片修复不产生事件。先取快照再追增量，两次请求之间的提交不会漏掉（详见下文「分片进度增量同步」）。

完成后的文件支持两类下载，均支持 HTTP `Range` 断点续传：

- **会话下载**（原有接口，不变）：`GET /sessions/{id}/download`，无次数限制；
- **受控下载票据**（新增）：可签发放发带**有效期（1～1440 分钟）**与**下载次数上限（1～100 次）**的一次性随机票据，凭票 URL 下载；服务端只保存票据的哈希，可随时撤销。

两条下载路径共用同一套 Range 语义（详见下文「下载成品」与「凭票下载」）：不带 `Range` 返回完整文件（`200` + `Accept-Ranges: bytes`）；带**单个**合法 `bytes` 区间（闭区间 `bytes=100-199`、开放结尾 `bytes=100-`、后缀 `bytes=-100`）按成品实际长度规范化后只流式输出对应字节，返回 `206` 与 `Content-Range`/`Content-Length`；多范围或越界请求返回 `416` 与 `Content-Range: bytes */总长度`，**不读取文件内容**。

已完成会话的成品还可随时接受**完整性巡检**：`POST /sessions/{id}/integrity-check` 流式重算成品的 SHA-256 并核对声明长度与预期摘要；一旦发现成品文件缺失、长度不符或摘要不符，立即持久化**隔离标记**并返回具体原因，此后该会话的会话下载、票据签发与凭票下载一律拒绝（凭票下载被拒时不扣减次数）；删除会话时标记随会话一并清除（详见下文「成品完整性巡检与隔离」）。

技术栈：Python 3.12 · FastAPI · SQLite（WAL 模式） · Uvicorn，容器化一键启动。

---

## 一键启动

需要 Docker 与 Docker Compose v2（`docker compose`）：

```bash
docker compose up --build -d
```

服务监听宿主机 **8000** 端口：

- 健康检查：<http://localhost:8000/health>
- 交互式文档（Swagger UI）：<http://localhost:8000/docs>
- 停止并清理：`docker compose down`（数据保留在命名卷 `resumable-data` 中；`-v` 连数据一起删除）

> 单容器单 uvicorn worker 部署。会话级互斥锁是进程内对象，多副本水平扩容需要额外的共享锁/存储，本版本不涉及。

---

## 配置项

全部配置通过环境变量覆盖（见 `docker-compose.yml` 的 `environment` 段）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据根目录（SQLite 库、分片、成品、临时文件） |
| `DATABASE_URL` | `sqlite:///$DATA_DIR/resumable.db` | SQLite 文件路径（形如 `sqlite:////path/to.db`） |
| `SESSION_TTL_SECONDS` | `86400`（1 天） | 会话存活时长；每次分片活动向后滑动（滑动过期） |
| `CLEANUP_INTERVAL_SECONDS` | `300`（5 分钟） | 后台清理线程的运行间隔 |
| `MAX_CHUNK_BYTES` | `67108864`（64 MiB） | 单个分片最大字节数，`0` 表示不限制 |
| `MAX_UPLOAD_BYTES` | `107374182400`（100 GiB） | 整文件最大字节数，`0` 表示不限制 |
| `MAX_FILENAME_LENGTH` | `255` | 文件名最大长度 |
| `CHUNK_CLAIM_TTL_SECONDS` | `300`（5 分钟） | 分片认领令牌的有效期；过期后区间可重新认领 |

数据目录布局：

```
$DATA_DIR/
├── resumable.db (+ -wal / -shm)     SQLite 数据库（upload_sessions / chunks / chunk_events / download_tickets / idempotency_keys / chunk_claims / quarantine_marks）
├── files/<session_id>               完成后的成品（原子 rename 落地）
├── parts/<session_id>/<start>-<end> 已接收分片（修复期间另有同名 .repairbak 备份，瞬时存在）
└── tmp/                             正在接收/拼装的临时文件（启动周期清理）
```

数据库结构通过 `PRAGMA user_version` 管理版本，**服务启动时自动建库/增量迁移**，老库数据保留：v1 为上传会话与分片表，v2 新增 `download_tickets`（受控下载票据表），v3 新增 `idempotency_keys`（创建会话幂等键表，仅存键的 SHA-256 与请求指纹），v4 新增 `chunk_claims`（分片认领令牌表，仅存令牌的 SHA-256），v5 新增 `chunk_events`（分片接收事件流：同会话序号从 1 严格连续单调递增；旧会话不回填事件，已有分片区间即游标 0 的快照基线），v6 新增 `quarantine_marks`（成品完整性巡检的隔离标记：每会话至多一条，删除会话时经外键级联一并清除）。

---

## API 约定

- Base URL：`/api/v1`
- 偏移量为字节偏移，采用**半开区间 `[start, end)`**，即 `end - start = 分片字节数`
- 分片可以按**任意顺序**提交，允许重连后只补传缺失部分
- 错误响应体：`{"detail": {"error": "<错误码描述>", ...附加上下文}}`

### 1. 创建上传会话

```http
POST /api/v1/sessions
Content-Type: application/json
Idempotency-Key: order-2026-09-20-0001   ← 可选

{
  "filename": "movie.mp4",
  "total_size": 104857600,
  "sha256": "9f1b...（64 位小写十六进制，允许 sha256: 前缀）",
  "chunk_size": 4194304
}
```

`chunk_size` 仅为客户端建议值，可为 0。返回 `201` 与完整状态（含 `session_id`、`expires_at`、初始 `missing_ranges`）。

**幂等创建（可选）**：携带 `Idempotency-Key` 头时，相同键 + 相同请求的重试返回**首次创建的会话**（同一 `session_id`，状态码仍为 `201`），不会重复建会话——首次响应丢失后的重试与并发重试都安全。

- 键格式：**1～128 个可见 ASCII 字符**（0x21–0x7E，不含空格），不合法返回 `400`。
- 服务端**只保存键的 SHA-256 与规范化请求指纹**（`filename`/`total_size`/`sha256`/`chunk_size` 规范化后的 SHA-256），明文键不落库。
- 相同键搭配**不同**请求（四个字段任一不同）→ `409`，且不新建会话。
- 键记录与会话在**同一事务**中创建；删除会话（`DELETE /sessions/{id}`）时键记录级联删除，之后同键请求按全新请求处理。
- 不带该头的请求行为不变：每次都创建新会话。

### 2. 提交二进制分片

```http
PUT /api/v1/sessions/{session_id}/chunks?start=0&end=4194304
Content-Type: application/octet-stream
Content-Length: 4194304
X-Chunk-SHA256: 3a7b...（可选，本分片字节的 64 位十六进制 SHA-256，允许 sha256: 前缀）

<原始二进制分片>
```

服务端校验（任一不过即拒绝，不写入任何分片数据）：

- 会话存在且处于 `uploading`（未过期/未放弃/未完成）
- `0 <= start < end <= total_size` —— 越界返回 `400`
- `Content-Length == end - start`，且实际请求体长度一致 —— 不一致返回 `400/413`
- 分片不超过 `MAX_CHUNK_BYTES` —— 超限返回 `413`
- `[start, end)` 与任何已收区间**不得重叠**（相邻允许，如已有 `[0,100)` 再传 `[100,200)`）—— 重叠返回 `409`

**`X-Chunk-SHA256`（可选，逐片校验）**：客户端若提供该头，服务端在流式接收分片的同时增量计算 SHA-256，并在**写入分片目录、登记区间之前**完成校验：

- 头格式错误（不是 64 位十六进制、长度不对、含非法字符等）→ `400 {"error":"invalid X-Chunk-SHA256 header"}`，在读取请求体之前拒绝，**不滑动 TTL、不落盘、不登记**；
- 请求体实际长度与区间不符 → 仍按长度错误返回 `400/413`（长度校验优先于摘要）；
- 长度正确但摘要不匹配 → `422 {"error":"chunk sha256 mismatch","expected":...,"actual":...,"start":...,"end":...}`，临时文件立即删除，**分片不落盘、不登记区间**；客户端用正确内容重传同一区间即可正常继续，最终整文件 SHA-256 校验不受影响。
- 不携带该头的请求**行为完全不变**（仅依赖完成时的整文件 SHA-256 兜底）；同一会话的各个分片可以自由选择是否带头。

**`X-Chunk-Claim`（可选，分片认领令牌）**：当 `[start, end)` 与一个**有效认领**（未过期、未消费）相交时，必须携带由 `POST /chunk-claims` 返回的令牌，且**令牌匹配、区间与认领完全一致**才接受；不带头返回 `409`，令牌伪造/不属于本会话返回 `403`，令牌过期返回 `410`，令牌有效但区间不一致返回 `409`。成功提交即在同一事务中消费令牌；长度/摘要错误或断流都**不消费**。未被认领的区间不带头即沿用原有行为（详见下文「分片认领」）。头格式非法（不是 `<claim_id>.<token>`）在读体之前返回 `400`。

curl 示例：

```bash
digest=$(sha256sum chunk-0.bin | awk '{print $1}')
curl -X PUT \
  -H "Content-Type: application/octet-stream" \
  -H "X-Chunk-SHA256: $digest" \
  --data-binary @chunk-0.bin \
  "http://localhost:8000/api/v1/sessions/$SID/chunks?start=0&end=4194304"
```

返回当前累计字节数与剩余 `missing_ranges`。并发提交同一区间时只有一个成功，其余得到 `409`。

### 2.1 服务端复制分片（copy-chunk）

```http
POST /api/v1/sessions/{session_id}/copy-chunk
Content-Type: application/json

{
  "source_session_id": "9f2c...",
  "source_start": 4194304,
  "source_end":   8388608,
  "target_start": 0
}
```

把**已完成来源会话成品**的半开区间 `[source_start, source_end)` 在服务端直接复制成目标会话从 `target_start` 开始的一个分片（目标区间长度 = `source_end - source_start`）。不经过客户端转发，适合“同一批字节要上传到多个新会话 / 重复前缀”的场景。

- 来源会话必须**已完成**（`status=completed`）且**成品文件存在**；来源未完成返回 `409 {"error":"source session is not completed"}`，来源不存在返回 `404`，库里标记完成但成品缺失返回 `409 {"error":"source completed artifact is missing"}`。
- 来源区间越界（`source_end > 来源 total_size`，或 `source_start >= source_end`）返回 `400/422`。
- 目标区间 `[target_start, target_start + 长度)` 越过目标 `total_size` 返回 `400`；单片超过 `MAX_CHUNK_BYTES` 返回 `413`；与目标已收区间重叠返回 `409`（相邻允许）；与目标会话的**有效分片认领**相交也返回 `409`（复制接口不携带认领令牌）。
- 来源会话与目标会话不得相同，否则 `400`；目标会话已完成时返回 `409`。
- **任一校验失败都不登记分片、不增加 `bytes_received`、不残留临时文件。**

复制成功的响应与普通 `PUT` 分片**完全一致**（`ChunkResponse`：`received` / `bytes_received` / `missing_ranges` / `fully_received` / `expires_at`），登记进同一张 `chunks` 表，因此后续仍按原有规则工作：必须**完整覆盖 + 整文件 SHA-256 校验通过**才能 `complete`，`PUT` 分片与 `copy-chunk` 可以在同一会话中任意混用。

实现上：复制期间同时持有来源、目标两把会话锁（按会话 ID 全局定序加锁，两个方向相反的并发复制不会死锁），先做全部预校验再流式复制到 `tmp/`，随后在单个 `BEGIN IMMEDIATE` 事务内二次校验、移动文件、登记区间与累加字节；事务任一步失败都会回滚并删除临时文件与未登记的落地文件，不留下已登记的损坏分片。复制期间来源无法被并发删除、目标无法被并发放弃。

curl 示例（服务监听 <http://localhost:8000>）：

```bash
curl -X POST http://localhost:8000/api/v1/sessions/$TARGET_SID/copy-chunk \
  -H "Content-Type: application/json" \
  -d '{"source_session_id":"'"$SOURCE_SID"'","source_start":4194304,"source_end":8388608,"target_start":0}'
```

### 2.2 已收分片原位修复（chunks/repair）

`complete` 返回 `422 sha256 mismatch` 时，说明某个已收分片的内容在传输/落盘过程中损坏。与其放弃会话重新上传，客户端可以先想办法定位损坏分片（例如客户端本地按相同偏移切分后比对摘要），再用**原位修复接口**把这一片替换成等长的正确内容：

```http
PUT /api/v1/sessions/{session_id}/chunks/repair?start=4194304&end=8388608
Content-Type: application/octet-stream
Content-Length: 4194304
X-Chunk-SHA256: 7c21...（可选，新内容的 64 位十六进制 SHA-256，允许 sha256: 前缀）

<与原分片等长的新二进制内容>
```

- 仅允许**上传中**（`uploading`）会话调用：会话不存在 → `404`；已过期/已放弃 → `410`；已完成 → `409`（完成后的会话不可改，且分片目录已在完成后清理）。
- `start`/`end`（半开区间 `[start, end)`）必须与某条**已登记分片完全一致**（同一个起止偏移）；只是部分重叠、相邻、或合并区间内跨分片边界都返回 `404 {"error":"no registered chunk matches the range exactly"}`。区间不存在时原文件、分片登记与已收字节数全部保留。
- 新内容长度必须等于 `end - start`：`Content-Length` 不符 → `400/411`；流式接收中实际超出 → `413`；单片超过 `MAX_CHUNK_BYTES` → `413`；`start >= end` → `400`。
- `X-Chunk-SHA256` 头的规则与普通 PUT 分片完全一致：格式错误在读体之前拒绝 → `400`；长度正确但摘要不符 → `422 {"error":"chunk sha256 mismatch",...}`，临时文件删除，**旧分片原封不动**。
- **任何失败（长度/摘要不符、会话失效、区间不存在）都不改变旧分片文件、chunks 登记、`bytes_received` 与 `missing_ranges`，也不滑动 TTL**；客户端修正请求后可直接重试，旧分片始终可用。

成功响应与普通 PUT 分片完全一致（`ChunkResponse`），但语义是**替换而非新增**：

- `received` 仍是被修复的 `{start, end}`；
- `missing_ranges`、`bytes_received`、`fully_received` 与修复前**完全相同**（登记的区间集合没有变化）；
- 成功后滑动活动时间（视为一次分片活动）；随后可再次 `POST /complete` 重新拼装并做整文件 SHA-256 校验。

实现上：首次校验通过后请求体流式写入 `tmp/`（弱网下不整分片进内存，可增量计算摘要）；提交阶段在会话锁内的单个 `BEGIN IMMEDIATE` 事务中复查会话状态与区间登记，随后按 **“旧分片同目录原子改名为备份 → 新分片原子 rename 落位 → 更新活动时间”** 的顺序完成替换，因此并发 `complete` 或 `DELETE`（放弃）只会看到旧分片或新分片，绝不会读到半替换内容。落盘或数据库事务任一步失败都会回滚事务并把旧分片从备份原子恢复，不留临时文件/备份；并发修复同一片串行执行，最后一次成功的内容为准（last-writer-wins）。进程崩溃残留的备份文件由后台清理线程定期回收。

curl 示例（服务监听 <http://localhost:8000>）：

```bash
SID=...                    # 上传中、且 [4194304,8388608) 已登记的会话
digest=$(sha256sum fixed-chunk-1.bin | awk '{print $1}')
curl -X PUT \
  -H "Content-Type: application/octet-stream" \
  -H "X-Chunk-SHA256: $digest" \
  --data-binary @fixed-chunk-1.bin \
  "http://localhost:8000/api/v1/sessions/$SID/chunks/repair?start=4194304&end=8388608"

# 修复成功（缺失区间/字节数不变）后重新完成整文件校验
curl -X POST "http://localhost:8000/api/v1/sessions/$SID/complete"
```

### 2.3 分片完整性诊断（chunks/audit）

`complete` 返回 `422 sha256 mismatch` 时，客户端知道文件某处损坏，却不知道坏在哪一片。诊断接口接收客户端本地算好的分片摘要清单，逐条与磁盘上的分片核对，直接圈定 `chunks/repair` 的目标区间：

```http
POST /api/v1/sessions/{session_id}/chunks/audit
Content-Type: application/json

{
  "chunks": [
    {"start": 0,       "end": 4194304, "sha256": "3a7b..."},
    {"start": 4194304, "end": 8388608, "sha256": "7c21..."}
  ]
}
```

- 仅允许**上传中**（`uploading`）会话调用：会话不存在 → `404`；已过期/已放弃 → `410`；已完成 → `409`。
- 清单 **1～32 条**；每条的 `{start, end}`（半开区间）必须与某条**已登记分片完全一致**（与 repair 同一判定），且区间不可重复；`sha256` 为 64 位十六进制（可带 `sha256:` 前缀、大小写不限，响应中回显规范化小写）。任一不合法**整批拒绝、一条都不核对**：结构/格式错误（条数超限、区间重复、`start >= end`、摘要非法）→ `422`，区间无精确匹配的已登记分片 → `404 {"error":"no registered chunk matches the range exactly","start":...,"end":...}`。
- 服务端**按起点顺序流式读取磁盘分片**并增量计算 SHA-256，响应按起点升序逐条返回核对结论：

```json
{
  "session_id": "e3a886dc...",
  "all_match": false,
  "results": [
    {"start": 0,       "end": 4194304, "status": "ok",
     "expected_sha256": "3a7b...", "actual_sha256": "3a7b...", "size_on_disk": 4194304},
    {"start": 4194304, "end": 8388608, "status": "sha256_mismatch",
     "expected_sha256": "7c21...", "actual_sha256": "91d0...", "size_on_disk": 4194304}
  ]
}
```

  - `ok`：磁盘分片长度与摘要都和清单一致；
  - `sha256_mismatch`：长度正确但摘要不符，`actual_sha256` 为磁盘实际摘要；
  - `length_mismatch`：磁盘文件长度与区间长度不符（`size_on_disk` 为实际长度，`actual_sha256` 按实际内容计算）；
  - `missing`：分片文件缺失（`actual_sha256` 与 `size_on_disk` 为 `null`）。
- 诊断是**纯只读**的：**不滑动 TTL、不改变分片登记与 `bytes_received`**，也不产生临时文件；全过程持有会话锁，修复/完成/放弃都无法在核对中途插入，不会把替换中的瞬时状态误报为缺失或损坏。
- 对 `status` 非 `ok` 的区间逐条调用 `PUT /chunks/repair` 修复即可（`missing` / `length_mismatch` 的区间登记仍在，按 repair 规则等长补写），随后重新 `POST /complete`。

curl 示例（服务监听 <http://localhost:8000>）：

```bash
SID=...          # 上传中的会话
D0=$(sha256sum chunk-0.bin | awk '{print $1}')   # 客户端本地按相同偏移切分计算
D1=$(sha256sum chunk-1.bin | awk '{print $1}')
curl -X POST "http://localhost:8000/api/v1/sessions/$SID/chunks/audit" \
  -H "Content-Type: application/json" \
  -d '{"chunks":[
        {"start":0,"end":4194304,"sha256":"'"$D0"'"},
        {"start":4194304,"end":8388608,"sha256":"'"$D1"'"}
      ]}'
# → 对 results 中 status 非 "ok" 的区间调用 chunks/repair，再重新 complete
```

### 2.4 分片认领（chunk-claims）

多个上传者（或多台机器）协作上传同一会话时，可用**分片认领**把尚未接收的区间临时占为己有：持令牌者拥有该区间的提交权，其他上传者提交相交分片会得到 `409`，避免大家重复上传同一段。认领是**一次性限时令牌**：

```http
POST /api/v1/sessions/{session_id}/chunk-claims
Content-Type: application/json

{"start": 4194304, "end": 8388608}
```

返回 `201`：

```json
{
  "claim_id": "1f2a3b4c...（32 位十六进制）",
  "session_id": "e3a886dc...",
  "start": 4194304,
  "end": 8388608,
  "created_at": "2026-09-24T10:00:00Z",
  "expires_at": "2026-09-24T10:05:00Z",
  "claim": "1f2a3b4c....9pQ2xZ7yK3vN8mR1wT6sU4aB0dE5fG7hJ9jL2kM4nP6"
}
```

- `expires_at - created_at` 恒为 `CHUNK_CLAIM_TTL_SECONDS`（默认 **300 秒 / 5 分钟**）。
- 服务端**只保存令牌明文的 SHA-256**，明文 `claim`（`<claim_id>.<token>`）仅在 `201` 响应中返回这一次，请像密码一样保管。
- 提交被认领区间时把令牌原样放进 `X-Chunk-Claim` 头即可：

```bash
SID=...                        # 上传中的会话
CLAIM=$(curl -s -X POST http://localhost:8000/api/v1/sessions/$SID/chunk-claims \
  -H "Content-Type: application/json" \
  -d '{"start":4194304,"end":8388608}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["claim"])')

curl -X PUT \
  -H "Content-Type: application/octet-stream" \
  -H "X-Chunk-Claim: $CLAIM" \
  --data-binary @chunk-1.bin \
  "http://localhost:8000/api/v1/sessions/$SID/chunks?start=4194304&end=8388608"
```

**认领创建规则**：

- 仅允许**上传中**（`uploading`）会话调用：会话不存在 → `404`；已过期/已放弃 → `410`（访问时懒过期）/`404`；已完成 → `409`。
- 半开区间 `[start, end)` 必须满足 `0 <= start < end <= total_size`，且不得超过 `MAX_CHUNK_BYTES`（认领的正是未来要提交的分片）：越界 → `400`，`start >= end` → `422`，超长 → `413`。
- 只能认领**尚未接收**的区间：与任何已登记分片相交 → `409 {"error":"claim range overlaps an already received range"}`（与已收区间相邻允许）。
- **相交的有效认领只能有一个**：与任何未过期认领相交（包含、被包含、部分重叠）→ `409 {"error":"range intersects an active claim",...}`；仅相邻（如已有 `[100,200)` 再认领 `[0,100)`）允许。
- **过期后可重领**：旧认领到期即不再参与相交判定（创建新认领时同事务回收旧行），同一区间可再次认领，旧令牌随之作废。
- 认领创建**不滑动会话 TTL**（它不是分片活动）。

**令牌与提交规则（PUT /chunks）**：

| 情形 | 结果 |
| --- | --- |
| 未带头，区间不与任何有效认领相交 | 正常接收（现有行为不变） |
| 未带头，区间与有效认领相交 | `409 chunk range intersects an active claim` |
| 令牌伪造/被篡改/属于其他会话/已被消费 | `403 invalid X-Chunk-Claim credential` |
| 令牌已过期 | `410 chunk claim expired`（区间随即对所有人开放，可重领或直接提交） |
| 令牌有效但区间与认领不完全一致 | `409 chunk range does not exactly match the claim` |
| 令牌匹配且区间完全一致 | `200`，成功登记即**消费（删除）令牌** |

- **成功提交消费令牌，失败或断流不消费**：消费与分片落盘登记在同一个 `BEGIN IMMEDIATE` 事务中完成——长度不符（`400`）、逐片摘要不符（`422`）、客户端断流、会话在接收期间结束等任何失败路径，令牌都原样保留，修正后可直接重试。
- 认领创建与分片落盘登记均在事务内**复核会话状态、已收区间与有效认领**：接收请求体期间他人新建的认领、过期的令牌都在提交事务中被再次判定，不存在预校验后的时间窗漏洞。
- `copy-chunk` 不携带令牌：复制到与有效认领相交的目标区间同样返回 `409`，未认领区间行为不变。

**会话结束时认领失效**：会话完成、过期（后台清理或访问时懒过期）时同事务删除其全部认领；放弃会话时认领随外键 `ON DELETE CASCADE` 级联删除。后台清理线程每轮还会全局回收已到期的认领行（仅释放空间，不影响判定——过期令牌在提交时本就被拒绝）。

### 3. 查询状态 / 缺失区间

```http
GET /api/v1/sessions/{session_id}
```

```json
{
  "session_id": "...",
  "filename": "movie.mp4",
  "total_size": 104857600,
  "status": "uploading",
  "bytes_received": 8388608,
  "fully_received": false,
  "complete": false,
  "missing_ranges": [{"start": 4194304, "end": 8388608}, {"start": 12582912, "end": 104857600}],
  "expires_at": "2026-09-21T12:00:00Z",
  "last_access_at": "2026-09-20T12:00:00Z",
  "completed_at": null,
  "download_url": null
}
```

客户端断连恢复时：先查状态，按 `missing_ranges` 补传即可。

### 3.1 分片进度增量同步（progress / events）

多个客户端（或同一客户端的多台设备）协作上传同一会话时，断线重连后需要追踪**其他客户端在自己离线期间成功提交了哪些区间**。服务端为每个会话维护一条只增不改的**分片接收事件流**：普通 `PUT /chunks` 与服务端 `copy-chunk` 每成功登记一个分片，就在**同一登记事务**内写入一条事件，事件序号（`seq`）在同会话内从 1 开始**严格连续、单调递增**；失败请求、重试时的重叠冲突（`409`）与 `chunks/repair` 原位修复**都不产生事件**（修复只换内容，区间集合不变）。

同步协议是“**先快照、再游标续读**”：

#### 进度快照

```http
GET /api/v1/sessions/{session_id}/progress
```

```json
{
  "session_id": "e3a886dc...",
  "status": "uploading",
  "total_size": 104857600,
  "bytes_received": 8388608,
  "received_ranges": [{"start": 0, "end": 4194304}, {"start": 8388608, "end": 12582912}],
  "missing_ranges": [{"start": 4194304, "end": 8388608}, {"start": 12582912, "end": 104857600}],
  "event_cursor": 3,
  "expires_at": "2026-09-25T10:00:00Z"
}
```

- `received_ranges`：当前全部已收区间（相邻区间已合并，按起点升序）——重连后的**基线**。
- `event_cursor`：该会话当前的事件游标（已生成的最大事件序号，无事件时为 `0`）。区间与游标在**同一个数据库读事务**中读出，取自同一一致性快照。
- 接口为**纯只读**：不滑动 TTL、不更新活动时间、不触发懒过期（与 `GET /storage/summary` 同一口径）。
- 会话不存在 → `404`；已过期/已放弃 → `410`；已完成会话仍可查询（`status="completed"`、`expires_at=null`）。

#### 按游标分页拉取事件

```http
GET /api/v1/sessions/{session_id}/events?cursor=3&limit=100
```

- `cursor`：非负整数，只返回 `seq` 严格大于该游标的事件；首次续读使用进度快照里的 `event_cursor`。
- `limit`：页大小，范围 **1～100**，默认 `100`；越界返回 `422`。
- 事件按 `seq` **升序**返回，每条即一次成功接收的区间：

```json
{
  "session_id": "e3a886dc...",
  "cursor": 3,
  "events": [
    {"seq": 4, "start": 4194304, "end": 8388608, "source": "upload", "created_at": "2026-09-24T10:01:00Z"},
    {"seq": 5, "start": 12582912, "end": 16777216, "source": "copy", "created_at": "2026-09-24T10:02:00Z"}
  ],
  "next_cursor": 5,
  "has_more": false
}
```

- `source`：`upload`（普通 PUT 分片）或 `copy`（服务端 copy-chunk）。
- `next_cursor`：本页最后一条事件的 `seq`（本页为空时为 `null`）；用它作为下一页的 `cursor`。
- `has_more`：游标之后是否还有超过本页的事件；为 `true` 时立即翻页，为 `false` 时可用 `next_cursor` 定时轮询等待新提交（新事件随时可能产生）。
- 会话不存在 → `404`；已过期/已放弃 → `410`。游标领先于最新事件时返回空页（`events=[]`、`has_more=false`），不是错误。

**为什么不会漏掉两次请求之间的提交**：分片登记在 `BEGIN IMMEDIATE` 事务内串行提交。任意一次成功登记 E 相对快照读只有两种时序——E 在快照读之前提交，则其区间已包含在快照的 `received_ranges` 基线中；E 在快照读之后提交，则其 `seq` 必然大于快照游标，会被 `events?cursor=<快照游标>` 读到。不存在“基线里没有、事件也读不到”的提交。基线（合并区间）与后续事件按区间并集叠加即得到最新已收集合（事件区间必然不与基线及更早事件重叠——重叠登记本就会被拒绝）。

**旧会话（升级前创建）**：不回填历史事件。迁移时已有的 `chunks` 区间即为游标 `0` 的快照基线；升级后首个成功登记的分片取得 `seq=1`，事件流从基线之后继续，协议与新会话完全一致。

curl 示例（服务监听 <http://localhost:8000>）：

```bash
SID=...
# 1) 断线重连：先取快照（基线 + 游标）
curl -s "http://localhost:8000/api/v1/sessions/$SID/progress" | python3 -m json.tool
# 2) 用快照游标追增量；has_more=true 就用 next_cursor 翻页，false 就轮询
curl -s "http://localhost:8000/api/v1/sessions/$SID/events?cursor=3&limit=100"
```

### 4. 原子完成

```http
POST /api/v1/sessions/{session_id}/complete
```

- 区间未全覆盖 → `409`，响应里带当前 `missing_ranges`，不产生成品
- 已覆盖但 SHA-256 不匹配 → `422`（返回 `expected`/`actual`），会话保持 `uploading`，客户端可修正后重试或放弃；不产生成品
- 全部通过 → 拼装临时文件 → 校验 → 与状态翻转在同一事务窗口内**原子发布**，状态变为 `completed`，开放下载。完成接口幂等。

### 5. 下载成品（会话下载，不计数）

```http
GET /api/v1/sessions/{session_id}/download
Range: bytes=1048576-2097151      ← 可选
```

仅 `completed` 会话返回成品；其他状态一律 `404`，失败流程中不存在任何可下载产物。该接口不校验票据、不限制次数。唯一的额外限制：会话被完整性巡检**隔离**后返回 `423 {"detail":{"error":"session quarantined","reason":...}}`（见下一节）。

**断点续传（HTTP Range）**：

- 不带 `Range`：返回 `200` 与完整文件，并始终携带 `Accept-Ranges: bytes`；
- 带**单个**合法 `bytes` 区间时只返回对应字节，状态码 `206`，并携带：
  - `Content-Range: bytes <起>-<止>/<总长度>`（止为闭区间最后一个字节的偏移），
  - `Content-Length: <本次字节数>`、`Accept-Ranges: bytes`；
- 支持的三种写法（均以成品**实际长度**规范化，半开区间长度即字节数）：

| 写法 | 含义 | 示例（长度 10000） |
| --- | --- | --- |
| `bytes=100-199` | 闭区间：字节偏移 100～199（含两端，共 100 字节） | `206`，`Content-Range: bytes 100-199/10000` |
| `bytes=100-` | 开放结尾：字节 100 到文件末尾（共 9900 字节） | `206`，`Content-Range: bytes 100-9999/10000` |
| `bytes=-1000` | 后缀形式：文件最后 1000 字节 | `206`，`Content-Range: bytes 9000-9999/10000` |

- 闭区间结尾越过文件末尾时按 RFC 收敛到末尾（如长度 10000 请求 `bytes=9900-99999` 返回 9900～9999，仍为 `206`）；后缀长度不小于文件长度时等价于整个文件（`206`）；
- **多范围**（一个头里逗号分隔多个区间）或**无法满足的范围**（起点越过文件末尾、区间倒置、语法非法、空文件上的任何区间）返回 `416 Range Not Satisfiable`，响应头携带 `Content-Range: bytes */<总长度>`；416 判定只取文件长度（`stat`），**绝不打开/读取成品内容**；
- 服务端始终以 1 MiB 分块**流式**输出区间字节，不会把成品整段读入内存。

curl 续传示例（服务监听 <http://localhost:8000>）：

```bash
SID=...                        # 已完成的会话
URL="http://localhost:8000/api/v1/sessions/$SID/download"

# 1) 断点续传：curl -C - 让 curl 根据已下载大小自动带上 Range 续传，
#    网络中断后重复执行同一条命令即可从断点继续（服务端返回 206）
curl -fL -C - -o movie.mp4 "$URL"

# 2) 手工指定区间：下载第 2 个 MiB（偏移 1048576～2097151）
curl -f -H "Range: bytes=1048576-2097151" "$URL" -o chunk-1.bin
# 响应头示例：
#   HTTP/1.1 206 Partial Content
#   Accept-Ranges: bytes
#   Content-Range: bytes 1048576-2097151/104857600
#   Content-Length: 1048576

# 3) 开放结尾（从偏移 1048576 到文件末尾）与后缀形式（最后 4 MiB）
curl -f -H "Range: bytes=1048576-" "$URL" -o tail-from-1m.bin
curl -f -H "Range: bytes=-4194304" "$URL" -o last-4m.bin
```

### 5.1 成品完整性巡检与隔离（integrity-check）

已完成会话的成品可能因磁盘损坏、误删等原因不再可信。巡检接口对成品**流式重算 SHA-256**（弱机也不会把整文件读进内存），并核对声明长度（`total_size`）与预期摘要（`sha256_expected`）：

```http
POST /api/v1/sessions/{session_id}/integrity-check
```

- 仅允许**已完成**（`completed`）会话调用：会话不存在 → `404`；已过期/已放弃 → `410`/`404`；仍在上传中 → `409 {"error":"session is not completed; no artifact to inspect"}`。
- **校验通过** → `200`，返回实际长度与摘要，**不改变会话的任何状态**（不滑动 TTL、不写库；若会话此前已被隔离，标记也原样保留——已完成会话的成品不可变，标记只能随会话删除消失）：

```json
{
  "session_id": "e3a886dc...",
  "status": "ok",
  "quarantined": false,
  "declared_size": 104857600,
  "size_on_disk": 104857600,
  "expected_sha256": "9f1b...",
  "actual_sha256": "9f1b...",
  "quarantined_at": null,
  "checked_at": "2026-09-25T12:00:00Z"
}
```

- **文件缺失 / 长度不符 / 摘要不符** → 仍为 `200`，`status` 分别为 `missing` / `length_mismatch` / `sha256_mismatch`（长度核对优先于摘要核对），同时**持会话锁在单个事务中持久化隔离标记**（`quarantine_marks` 表，每会话至多一条；重复巡检刷新原因与时间，不堆叠），响应带回具体原因与标记时间：

```json
{
  "session_id": "e3a886dc...",
  "status": "sha256_mismatch",
  "quarantined": true,
  "declared_size": 104857600,
  "size_on_disk": 104857600,
  "expected_sha256": "9f1b...",
  "actual_sha256": "aa00...",
  "quarantined_at": "2026-09-25T12:00:00Z",
  "checked_at": "2026-09-25T12:00:00Z"
}
```

  文件缺失时 `size_on_disk` 与 `actual_sha256` 为 `null`。

**隔离的效力**：会话被隔离后——

- `GET /sessions/{id}/download`（会话下载）→ `423 {"detail":{"error":"session quarantined","reason":...}}`；
- `POST /sessions/{id}/tickets`（签发新票据）→ `423`；
- `GET /tickets/{ticket}/download`（凭票下载，含此前已签发的票据）→ `423`，且判定发生在原子扣减**之前**，**绝不扣减剩余次数**；
- 状态查询（`GET /sessions/{id}`）、票据撤销等管理操作不受影响。

**标记的生命周期**：巡检与删除会话（`DELETE /sessions/{id}`）按**同一把会话锁**串行——删除先则巡检得到 `404`、不写任何标记；巡检先则删除排队，随后删除时标记经外键 `ON DELETE CASCADE` 与会话行在**同一事务**中清除。已删除的会话绝不会被重新写入隔离标记。

curl 示例（服务监听 <http://localhost:8000>）：

```bash
SID=...          # 已完成的会话
curl -X POST "http://localhost:8000/api/v1/sessions/$SID/integrity-check" | python3 -m json.tool
```

### 6. 签发受控下载票据

```http
POST /api/v1/sessions/{session_id}/tickets
Content-Type: application/json

{
  "expires_in_minutes": 60,
  "max_downloads": 5
}
```

- `expires_in_minutes`：有效期分钟数，范围 **1～1440**；`max_downloads`：下载次数上限，范围 **1～100**。越界返回 `422`。
- 仅 `completed` 会话可签发，否则 `409`；会话已被巡检**隔离**时拒绝签发，返回 `423 {"detail":{"error":"session quarantined","reason":...}}`。
- 服务端生成约 256 bit 熵的随机票据，**数据库只保存其 SHA-256**，明文仅在 `201` 响应中返回这一次，服务端无法再次展示，也无法从库中反推：

```json
{
  "ticket_id": "777aaac7...",
  "session_id": "e3a886dc...",
  "created_at": "2026-09-20T16:00:51Z",
  "expires_at": "2026-09-20T17:00:51Z",
  "max_downloads": 5,
  "remaining_downloads": 5,
  "revoked": false,
  "download_url": "/api/v1/tickets/777aaac7.../download",
  "ticket": "777aaac7....Uw21NON6si8F-Ri4B2HKWFXMCcDkY4wY3u_eyaR8rmQ"
}
```

`ticket` 即下载凭证（`<ticket_id>.<随机串>`，URL-safe），把它拼到下载路径即可分发；请像对待密码一样保管它，票据 URL 可能出现在代理/访问日志中。

### 7. 凭票下载（原子计数，支持 Range）

```http
GET /api/v1/tickets/{ticket}/download
Range: bytes=100-199
```

服务端在**单个 SQLite `BEGIN IMMEDIATE` 事务**内完成全部校验与扣减：

1. 票据存在（id 与随机串的哈希同时匹配），否则 `404`；
2. 未撤销，否则 `403`；
3. 未过期（当前时间 ≤ `expires_at`），否则 `410`；
4. `remaining_downloads > 0` 且会话成品仍存在，否则 `429` / `404`；
5. 会话未被巡检隔离，否则 `423`（判定在扣减之前，**不扣减次数**）；
6. `Range` 头（若携带）按成品实际长度规范化：单个合法区间才放行，多范围/越界/语法非法直接 `416`（响应头 `Content-Range: bytes */<总长度>`），该步只需文件长度、**不读取成品内容**。

校验通过后用条件 `UPDATE ... WHERE revoked=0 AND expires_at>? AND remaining_downloads>0` 原子扣减一次再提交。并发请求在事务内串行，后到者看到的剩余次数为 0，**不可能突破上限**；任何校验失败都在扣减前回滚，**绝不计数**。

- 不带 `Range` 的完整下载返回 `200`；**单个合法** `Range`（闭区间 `bytes=100-199`、开放结尾 `bytes=100-`、后缀 `bytes=-100`，结尾越界按 RFC 收敛）返回 `206` 与 `Content-Range`/`Content-Length`/`Accept-Ranges: bytes`——**合法 Range 同样按一次计数**（下载器的断点续传会消耗票据次数）。
- **范围非法（`416`）不扣减次数**：多范围、起点越过文件末尾、区间倒置、语法非法等请求在原子扣减之前被拒，修正范围后重试次数不变；空文件成品上的任何 Range 都是 `416`。
- 会话已隔离时，即使携带合法 Range 也返回 `423` 且不扣次（隔离判定先于 Range）。
- 次数用尽后返回 `429 {"detail":{"error":"ticket download limit reached"}}`。
- 票据独立计数，互不影响；删除所属会话会级联删除其票据。

curl 续传示例（票据 URL 与会话下载的 Range 语义完全一致）：

```bash
TICKET='777aaac7....Uw21NON6si8F-Ri4B2HKWFXMCcDkY4wY3u_eyaR8rmQ'
URL="http://localhost:8000/api/v1/tickets/$TICKET/download"

# 断点续传：中断后重复执行即可；注意每次成功的 200/206 都会消耗一次票据次数，
# 因此请按预期续传次数签足 max_downloads；范围非法的 416 不消耗次数
curl -fL -C - -o movie.mp4 "$URL"

# 手工续传某一段（206，扣减一次）
curl -f -H "Range: bytes=1048576-" "$URL" -o tail.bin
```

### 8. 撤销票据

```http
POST /api/v1/tickets/{ticket}/revoke
```

幂等：已撤销再次调用仍返回 `200` 与票据元数据（`revoked: true`，剩余次数原样保留）。票据不存在/伪造返回 `404`。撤销后任何凭票下载立即得到 `403`，且不扣减。

### 9. 放弃会话

```http
DELETE /api/v1/sessions/{session_id}
```

幂等，返回 `204`：删除会话记录、已收分片与成品。

### 10. 存储汇总（运维只读）

```http
GET /api/v1/storage/summary
```

在**单个 SQLite 只读快照**中按会话状态汇总，供运维容量观测。响应固定包含 `uploading`、`completed`、`aborted`、`expired` 四行（即使计数为零也保留），外加由四行求和得到的 `total` 行：

```json
{
  "statuses": [
    {"status": "uploading", "session_count": 3, "declared_bytes": 210, "stored_bytes": 130},
    {"status": "completed", "session_count": 1, "declared_bytes": 10,  "stored_bytes": 10},
    {"status": "aborted",   "session_count": 0, "declared_bytes": 0,   "stored_bytes": 0},
    {"status": "expired",   "session_count": 0, "declared_bytes": 0,   "stored_bytes": 0}
  ],
  "total": {"status": "total", "session_count": 4, "declared_bytes": 220, "stored_bytes": 140}
}
```

字段口径：

- `declared_bytes`：该状态所有会话 `total_size`（客户端声明大小）之和。
- `stored_bytes`：实际占用的逻辑字节数——`uploading` 取 `bytes_received` 之和，`completed` 取 `total_size` 之和，`aborted` / `expired` 恒为 `0`。

该接口是**纯只读**的：以 `mode=ro` + `PRAGMA query_only` 的独立连接执行单条聚合 SQL，**不扫描文件系统、不更新 `last_access_at`、不滑动会话 TTL，也不触发访问时懒过期**（已过 `expires_at` 但尚未被后台清理的会话仍按 `uploading` 统计）。

### 状态码与生命周期

| 状态 | 含义 |
| --- | --- |
| `uploading` | 收片中；超过 `expires_at` 未活动则过期 |
| `completed` | 校验通过、成品可下载 |
| `expired` | 已被懒过期或后台清理标记，再提交分片得到 `410`，分片已清除 |
| `aborted` | 已放弃（删除接口直接硬删除，正常不可见） |

---

## 过期与清理

- **滑动过期**：创建会话按 `SESSION_TTL_SECONDS` 生成 `expires_at`；每次成功的分片活动自动顺延。
- **后台清理线程**：每 `CLEANUP_INTERVAL_SECONDS` 执行一轮，把过期会话标记为 `expired`、删除其分片目录，并清理已完成会话的分片、孤儿目录和崩溃残留的临时文件。
- **访问时懒过期**：即使后台线程尚未轮到，对已过期会话的任何操作都会立即得到 `410` 并触发清理。
- 已完成的成品**不**随会话过期删除。
- 下载票据的过期与次数在每次凭票下载时于事务内即时校验，过期/撤销的票据不依赖后台清理；删除会话时票据随外键 `ON DELETE CASCADE` 一并删除。
- 分片认领的过期在创建新认领与提交分片时于事务内即时判定（过期令牌不可用、过期区间可重领）；会话完成/过期/放弃时认领随会话结束删除，后台清理线程每轮再全局回收已到期的认领行。
- 成品完整性巡检的隔离标记只由巡检失败路径写入（每会话至多一条，重复巡检刷新）；删除会话时标记随外键 `ON DELETE CASCADE` 与会话行同事务清除，巡检与删除按同一把会话锁串行，已删除会话不会被重新写入标记。

## 弱网客户端建议

- 分片大小取 256 KiB～4 MiB；上传失败时按指数退避重试同一分片（重复提交重叠分片会得到 `409`，可直接忽略并查状态）。
- 恢复后先调 `GET /sessions/{id}`，只补 `missing_ranges`。
- 多端协作、断线重连后优先走**快照 + 增量**：先 `GET /sessions/{id}/progress` 拿到已收区间基线与 `event_cursor`，再用 `GET /sessions/{id}/events?cursor=<游标>` 追其他客户端新提交的区间（只含成功登记，失败/冲突/修复不出现），按区间并集合并后只补剩余缺口；`has_more=false` 时可用 `next_cursor` 轮询等待新事件。
- 多个上传者协作同一会话时，先 `POST /chunk-claims` 认领各自负责的区间（5 分钟内有效），再带 `X-Chunk-Claim` 令牌提交，避免重复上传同一段；认领过期或提交失败（令牌不消耗）后重新认领即可。
- 可选为每个分片附带 `X-Chunk-SHA256` 头：弱网/代理导致的分片片内损坏在提交时即可被 `422` 发现并就地重传，不必等到最后整文件校验失败后放弃整个会话。
- 全部补齐后调 `complete`；若得到 `422 sha256 mismatch`，说明某个已收分片内容损坏：先调 `POST /chunks/audit` 提交本地算好的分片摘要清单定位损坏分片（逐条返回匹配/摘要不符/文件缺失/长度不符与实际摘要），再对非 `ok` 区间用 `PUT /chunks/repair` 原位替换为等长正确内容（缺失区间与已收字节数不变），然后重新 `complete`，无需重新创建会话。

---

## 本地开发与测试

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pytest httpx
DATA_DIR=./data uvicorn app.main:app --reload --port 8000

# 测试（自动使用临时数据目录）
python3 -m pytest -q
```

测试覆盖：乱序完整上传与下载、越界/重叠/长度不符拒绝、缺失区间计算、并发重叠竞态（仅一个成功）、SHA-256 不匹配、空文件、放弃、过期清理与未知会话；受控票据覆盖参数边界、只存哈希、完整/Range 下载计数、耗尽 `429`、过期 `410`、撤销 `403`、伪造票据 `404`、30 并发不突破上限、票据相互独立、删除会话级联失效；幂等键覆盖同键重放返回同一会话、同键不同请求 `409`、键格式边界（1/128 字符、空、空格、非 ASCII）、只存哈希、10 并发同键重试只建一个会话、8 并发同键不同请求恰一个成功、删除会话级联删除键记录，以及 v1/v2 老库自动迁移到最新结构。`X-Chunk-SHA256` 覆盖匹配摘要接受（小写/大写/`sha256:` 前缀）、摘要不符 `422`（不落盘、不登记、可重传后正常完成整文件校验）、头格式错误 `400`（读体前拒绝）、长度错误优先于摘要错误、无头行为不变，以及带头/不带头分片混合上传。`copy-chunk` 覆盖区间复制后与 PUT 分片混传并通过整文件 SHA-256、整片复制、复制到不同目标偏移、来源与目标不同大小、来源可继续下载；拒绝未完成/不存在/成品缺失的来源、来源区间越界、目标越界、来源目标相同、与已收区间重叠（相邻允许）、单片超限（413）、目标已完成、目标未知；失败不登记不增字节不留临时文件；数据库登记失败（注入故障）回滚并清理落地文件后可重试、复制期间成品被删返回 409；反向并发复制不死锁、同区间并发只有一个成功、并发删除来源/放弃目标两种结局均不产生损坏登记。`chunks/repair` 覆盖损坏分片修复后重新通过整文件校验（带/不带摘要头、修复后再修复回原内容）、成功响应的缺失区间与字节数不变、区间必须与已登记分片完全一致（未登记/部分重叠/跨边界 404、非法区间 400）、Content-Length 不符 400、流式超长 413、单片超限 413、摘要不符 422（旧分片/登记/字节数/缺失区间保留、可重试后完成）、头格式错误 400、任何失败不滑动 TTL、未知会话 404、已完成 409（成品不变）、过期 410、越界 400、登记仍在但磁盘分片丢失时修复补齐；注入故障使新分片落位后的事务 UPDATE 失败时旧分片原子恢复且可重试成功；complete 在修复流式接收期间先完成（用旧内容发布、修复 409）、abort 在修复流式接收期间生效（修复 404/410、目录与临时文件清空）、双修复并发均 200 且最终内容恰为其一，以及后台清理回收崩溃残留的 `.repairbak` 备份。`chunks/audit` 覆盖全部匹配（乱序清单按起点升序返回）、四种状态混合（ok/摘要不符/长度不符/文件缺失，含实际摘要与实际长度）并据此圈定 repair 目标后完成整文件校验、清单子集、`sha256:` 前缀与大写摘要规范化、区间无精确匹配整批 404（未登记/部分重叠/超集/子段）、重复区间与条数超限（0/33 条）及摘要格式非法 422、未知会话 404、已完成 409、已过期 410、已放弃 404、诊断不滑动 TTL 且不改登记/已收字节/不留临时文件，以及诊断持锁期间 complete 排队、修复提交持锁期间诊断排队且放行后读到修复后的新内容。`chunk-claims` 覆盖认领成功响应（默认 5 分钟到期、令牌形态、只存哈希）、持令牌提交并消费后可正常完成、未认领区间行为不变、越界/非法区间/超单片上限/与已收区间相交的拒绝、相交有效认领互斥（相邻允许）、过期后重领且旧令牌不可用（403/410）、伪造/篡改/跨会话令牌 403、区间不完全一致 409、头格式错误 400、长度/摘要失败与断流不消费令牌、未知/已完成/已过期/已放弃会话的认领拒绝、放弃级联删除、过期与完成时认领删除、后台清理回收过期认领行、copy-chunk 遇有效认领 409、8 并发认领同一区间恰一个成功、认领不滑动会话 TTL，以及 v3 老库自动迁移到 v4。分片进度增量同步覆盖快照（空会话基线与游标 0、相邻区间合并、不滑动 TTL）、事件按提交顺序记录且 seq 无洞、游标位置续读与领先游标空页、分页边界（limit 0/101/负数 422、满页与末页 `has_more`/`next_cursor`）、序号按会话独立递增、12 并发提交序号连续唯一、快照后游标续读不漏提交、copy-chunk 记 `source=copy` 事件、重叠冲突/长度错误/摘要不符/原位修复均不产生事件，完成后快照与事件可读、放弃级联删除、过期 410、未知 404，以及旧会话已有 chunks 作为游标 0 基线、首个新分片从 seq=1 续号、v4 老库自动迁移到 v5（老数据保留、不回填事件、(session_id, seq) 唯一约束、迁移幂等）。成品完整性巡检覆盖健康成品通过（返回实际长度与摘要、会话状态逐字段不变、不写库、空文件成品）、成品缺失/长度不符/摘要不符三种隔离（具体原因与实际值回显、标记持久化、每会话至多一条重复巡检刷新）、隔离后会话下载/票据签发/凭票下载均 423（凭票下载不扣减次数、撤销票据等管理操作不受影响、状态查询正常）、复检通过不清除已有标记、上传中 409、未知 404、过期 410、删除会话清除标记且删除后巡检 404 不重写标记、巡检与删除按同一会话锁串行（删除排队等待巡检放行），以及 v5 老库自动迁移到 v6（老数据保留、标记可写读、迁移幂等）。成品断点下载（`ranges` 模块，会话下载与凭票下载共用）覆盖 Range 头规范化的闭区间/开放结尾/后缀三种形态（前导零、后缀不短于文件、闭区间结尾越界收敛、OWS 空白剥除）、无 Range 返回完整文件（200 + `Accept-Ranges`/`Content-Length`、无 `Content-Range`）、206 的 `Content-Range`/`Content-Length`/`Accept-Ranges` 逐头校验、大于 1 MiB 的区间跨块流式输出字节完整、多范围/起点越界/区间倒置/语法非法/空文件 Range 返回 416 与 `bytes */总长度` 且不输出成品字节（文件内容不可读时仅凭 stat 仍返回 416，证明不读内容）、隔离会话携带合法 Range 仍 423、空文件完整下载 200 而 Range 416；票据侧覆盖合法 206/200 各原子扣减一次（含扣尽后 429）、多范围/越界/非法 Range 的 416 不扣次且随后仍可下载满额、仅剩 1 次时连续 416 不吞次数、隔离会话上任何 Range 均 423 不扣次、空文件 Range 416 不扣次、撤销票据携带任意 Range 均 403 不扣次。
