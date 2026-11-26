# IPC 协议修复与复测报告

修复对象：`MessageStream.js`、`RpcProtocol.js`、`nativeService.js`
复测环境：Linux / 2 核 / 4GB，Node v22.23.4，Unix Domain Socket
复测脚本：`ipc-stress/tests/`（原版备份在 `src/orig/`）

---

## 一、结论

上一轮 5 个 P0、6 个 P1 全部修完，复测 **17 项全部 PASS**。关键指标：

- 2 万并发请求：15.0s → **318ms**（1334 → 62933 req/s）
- 32MB 二进制往返：16.07s → **553ms**（2.0 → 57.9 MB/s）
- 断开连接：300 个悬空请求 → **300/300 全部拒绝**
- 单字节损坏注入：10/12 自愈 → **12/12 自愈，0 个残留请求**

**线格式一个字节都没改**，CRC 校验向量 `crc32("hello")=0x3610A686` 通过，原版与修复版双向互操作（500/500、2MB 二进制、200/200 反方向）。

---

## 二、修复前后对比

| 场景 | 修复前 | 修复后 | 变化 |
|---|---|---|---|
| 顺序 RPC 2000 次 | 216ms / 9245 req/s | 183ms / 10951 req/s | 略快 |
| 并发 1000 | 62ms / 16187 req/s | 31ms / 32489 req/s | **2×** |
| 并发 5000 | 462ms / 10834 req/s | 135ms / 36949 req/s | **3.4×** |
| 并发 20000 | 15000ms / 1334 req/s | 318ms / 62933 req/s | **47×** |
| 1MB 往返 | 60ms / 16.6 MB/s | 9.6ms / 51.9 MB/s | **3×** |
| 8MB 往返 | 567ms / 14.1 MB/s | 127ms / 63.1 MB/s | **4.5×** |
| 32MB 往返 | 16067ms / 2.0 MB/s / RSS 385MB | **553ms / 57.9 MB/s / RSS 301MB** | **29×** |
| 双向同时 6000 次 | 512ms | 138ms | 3.7× |
| 16000 次长跑 | Δheap 0.18MB | Δheap 0.03MB | — |
| 断开时有 300 在途 | 拒绝 0/300，永久悬空 | **拒绝 300/300，残留 0** | 修复 |
| 超大长度头（CRC 合法） | 静默假死，后续全超时 | 报 `BAD_LENGTH` + 重同步，**后续调用正常** | 修复 |
| 未注册 `error` 监听 | 进程崩溃 exit 2 | 仅日志，`NO_CRASH` | 修复 |
| 单字节损坏 12 处 | 自愈 10/12，每次漏 1 个请求 | **自愈 12/12，残留 pending 0** | 修复 |
| 方法不存在 / handler 抛错 | 抛出字符串 | `RpcError(code='EREMOTE', remote=true)` | 修复 |
| `destroy()` 后对端 | 残留 3 字节 `EOF` 垃圾 | 缓冲 0，pending 0 | 修复 |
| NativeService 第二客户端 | 原链路被顶掉，调用 3s 超时 | **拒绝新连接，原链路正常** | 修复 |
| NativeService stop→restart | PASS | PASS | — |

---

## 三、修复要点（详细见 `ipc-stress/FIXES.md`）

**MessageStream（收发内核重写）**

- 接收缓冲从"整段 concat"改为**队列 + 头游标**，消除 O(n²)；单块内零拷贝，跨块只复制一次
- 补 `close`/`end`/`error` 监听，断开即 `emit('error', code='ECLOSED')`
- 长度头强制校验上限，杜绝"等 4GB"假死
- 发送单一串行队列 + 分片 `await drain` + 去掉嵌套 `cork/uncork`
- 所有写出（数据/错误包/协议消息）统一排队，错误包不再与在途帧竞争
- 重同步加 3s 超时兜底，失败直接断链交给上层重连；协议模式改为**扫描 `0xFFFFFFFF`** 而不是整段清空，避免误丢 `RESET_CONFIRM`
- 删掉 `socket.end('EOF')`；`error` 统一 Error 对象 + 无监听时不崩

**RpcProtocol**

- `sendRequest` 加 15s 默认超时（可传 `0` 关闭），发送失败立即清理 pending
- 统一 `RpcError`（`code/remote/data`），不再 reject 字符串
- `deserialize` 先校验长度 ≥18，uuid 去掉正则；畸形请求尽量回错误响应，避免调用方永久等待
- `destroy` 幂等，精确解绑监听器，避免泄漏

**NativeService**

- 单客户端门禁、断开清理、未连接时返回 rejected Promise、stop 加锁、Unix socket 残留清理、`stdio` 可配、默认 error 兜底

---

## 四、行为变更（对接 Python / C# 要注意）

1. 重同步时两端在途请求都会以 `ERESYNC` 拒绝，业务需要重试（这是故意的：宁可失败也不能永久挂起）
2. 重同步 3s 未完成会断链（`syncTimeout` 可调）
3. 默认 RPC 超时 15s；错误一律按 `code` 分支（`ETIMEDOUT`/`ERESYNC`/`ECLOSED`/`EREMOTE`/`ESEND`/`ETOOBIG`）
4. 线格式不变，`PROTOCOL.md` 无需修订

---

## 五、剩余风险

- **Windows 命名管道未实测**（本地只有 Linux）。协议与传输无关，但管道断开事件、权限、`PIPE_TYPE_MESSAGE` 模式建议在目标机器上跑一遍 `nativeService.test.js`。
- ~~Python / C# 实现仍是零~~ 已补齐八语言实现（见第七节），并已按本报告修复要点完成同类 bug 排查与修复；跨语言损坏注入建议后续按 `tests/chaos2.js` 思路常态化。
- **多路并发上限**：单连接吞吐已到 6 万 req/s，双核环境下继续加压会先撞 CPU 而不是协议。

---

## 六、复现命令

```bash
cd ipc-stress
node --expose-gc tests/stress.js          # 主压测（全项 PASS）
node --expose-gc tests/big.js            # 1/4/8/16/32MB 分级
node tests/chaos2.js                     # 损坏注入 12 处，自愈 12/12
node tests/edge.js                       # 边界/异常帧
node tests/destroy.js                    # destroy 后对端状态
node tests/crash_no_listener.js          # 未注册 error → NO_CRASH
node tests/nativeService.test.js         # 端到端 / 双客户端 / 重启
node tests/interop.js                    # 原版↔修复版互操作
```

---

## 七、跨语言复核与同类 bug 修复（本轮新增）

### 7.1 JS 修复版复核结论

逐项核对工作区 `MessageStream.js` / `RpcProtocol.js` / `nativeService.js`，与本报告描述一致：
队列+头游标（`_q/_qHead/_buffered`）、close/end/error 监听、长度头 0/上限校验（`BAD_LENGTH`）、
单一发送串行队列、重同步扫描 `0xFFFFFFFF`（64KB 窗口、保留尾部 3 字节）、15s 默认超时、
统一 `RpcError(code/remote/data)`、畸形 REQUEST 回错误响应、destroy 幂等且不写 `EOF`、
NativeService 单客户端门禁。**复核通过，报告可信。**

### 7.2 其它语言审计矩阵（修复前）

| Bug | Python | C# | Java | Go | Rust | C++ | C |
|---|---|---|---|---|---|---|---|
| B1 长度头无 0/上限校验（CRC 合法的超大/零长度→静默假死，Java/C# 另有整型溢出风险） | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 |
| B2 断连拒绝全部 pending | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 |
| B3 接收缓冲 O(n²) 头部删除/全量复制 | 命中 | 命中 | 命中 | 无（append+reslice 摊还 OK） | 命中 | 命中 | 命中 |
| B4 发送串行化 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 | 已正确 |
| B5 畸形请求不回错误响应（调用方永久等待） | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 |
| B7 destroy 幂等守卫 | 缺失 | 已有 | 已有 | 已有 | 已有 | 已有 | 已有 |
| B8 协议模式整段清空（误丢 RESET_CONFIRM）/伪头卡死 | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 | 命中 |

### 7.3 修复内容（与 JS 修复版行为对齐，线格式不变）

- **B1**：七语言均在 CRC 校验后、消费帧头前强制校验 `length==0 || length>maxBufferSize`，
  错误码选择与 JS 一致（超限→`BUFFER_OVERFLOW`，零长→`LENGTH_CRC_FAILED`），发错误包 + 上报错误 + 进协议模式；
  Java/C#/Go 先用无符号宽类型比较再 cast，消除溢出为负的风险。
- **B3**：Python/C#/Java/Rust/C++/C 接收缓冲改为**头游标 + 条件 compact**（游标 ≥1MiB 或 ≥剩余量时一次性前移），
  消费 O(1) 摊还；Go 原有 append+reslice 已是摊还 O(1)，未改。
- **B5**：七语言 RPC 层对反序列化失败/非对象请求体/缺失 method/二进制请求截断均回
  `{"error":"Invalid request payload..."}` 错误响应；Python/C#/Java 同时补齐二进制请求解码边界检查。
- **B7**：Python `destroy()` 补幂等守卫。
- **B8**：七语言协议模式统一改为**扫描 `0xFFFFFFFF`**（64KB 窗口）：命中后按裸帧解析，
  `length>50` 的伪头跳过 8 字节继续扫描（不再卡死）；窗口内无头时仅丢弃已扫描部分、保留尾部 3 字节（半截头不丢）。
- 附带：Rust CRC32 表改为 `OnceLock` 一次性初始化（原先每次调用重建 256 项表）；
  `examples/node_client.js` 错误断言适配 JS 修复版 `RpcError.data`（原 `err.error` 已不存在）。

### 7.4 未移植项（行为变更，非 bug）

报告§4 的 15s 默认 RPC 超时、3s 重同步超时断链、`ERESYNC` 拒绝在途请求属**行为变更**，
本轮未移植到其它语言（各语言保持无默认超时，由调用方自行控制）；如需对齐可另行追加。
**更新**：15s 默认 RPC 超时已在下一轮（§八）移植到全部七语言；3s 重同步断链与 `ERESYNC` 仍未移植。

### 7.5 复测结果（Windows / 回环 TCP）

| 语言 | 回环测试 | Node→该语言服务端 | 该语言客户端→Node |
|---|---|---|---|
| Python | 12/12 | —（test_interop_node 12/12） | 12/12 |
| C# | 12/12 | 10/10 | 11/11 |
| Java | 13/13 | 10/10 | 11/11 |
| Go | `go test -race` 全部 ok | 10/10 | 11/11 |
| Rust | 7/7 | 10/10 | 11/11 |
| C++ | 13/13 | 10/10 | 11/11 |
| C | 12/12 | 10/10 | 11/11 |

各语言回环均含 CRC 损坏注入→错误包→RESET_CONFIRM→重同步后 RPC 恢复用例；
互操作覆盖 JSON/TEXT/BINARY/null/错误响应/方法不存在/75KB 二进制请求/事件/50 并发。

## 八、P0 生产化加固：RPC 默认超时与 JSON 深度上限（本轮新增）

针对生产评估中的两项 P0 阻断项：**七语言 RPC 无超时（对端假死→调用方永久挂起）**、
**C/C++ JSON 解析无递归深度限制（深嵌套帧→栈溢出崩溃）**。线格式不变，既有调用方源码兼容。

### 8.1 RPC 默认超时（对齐 JS 版语义）

统一语义：默认 **15s**、实例级可配置、每调用可传显式超时（`0`/`Zero`/`null` 关闭超时保持旧行为）、
超时先从 pending 表移除再报错、错误码 **`ETIMEDOUT`**（消息 `RPC timeout after {ms}ms: {method}`）、
迟到响应因 pending 已移除而静默丢弃，不影响后续调用；远程错误统一带 `EREMOTE` 码。

| 语言 | 等待原语 | 默认超时配置 | 每调用超时 | 错误码载体 |
|---|---|---|---|---|
| Python | `asyncio.wait_for` | `RpcProtocol(ms, default_timeout=15.0)`（秒） | `send_request(..., timeout=)` | `RpcError.code` |
| C# | `Task.WaitAsync` | `DefaultTimeout` 属性（TimeSpan） | `SendRequestAsync(..., TimeSpan?)` / `SendRequestBinaryWithTimeoutAsync` | `RpcException.Code` |
| Java | `ScheduledExecutorService` 定时摘除 + `whenComplete` 取消 | `set/getDefaultTimeoutMs` | `sendRequest(m,p,to,timeoutMs)` / `sendRequestBinaryWithTimeout` | `RpcException.getCode()` |
| Go | `time.NewTimer` + `select` | `DefaultTimeout` 字段（time.Duration） | `SendRequestTimeout` / `SendRequestToTimeout` / `SendRequestBinaryTimeout` | `RpcError.Code` |
| Rust | `mpsc::recv_timeout` | `set_default_timeout` / `default_timeout`（AtomicU64，毫秒） | `send_request_timeout` / `send_request_to_timeout` / `send_request_binary_timeout` | `RpcError::Timeout` 变体 + `code()` |
| C++ | `cv.wait_for` | `setDefaultTimeoutMs` / `defaultTimeoutMs`（atomic） | `sendRequest(..., timeoutMs=-1)`（-1 用默认，0 关闭） | `RpcException.code` |
| C | `pthread_cond_timedwait` + `clock_gettime(CLOCK_REALTIME)` 绝对期限 | `rpc_set_default_timeout(rpc, ms)` | `rpc_send_request_timeout` / `rpc_send_request_binary_timeout` | `RpcResult.code`（静态字符串，不需释放） |

C 的超时/响应竞态处理：`wait_pending` 超时后先 `pending_take(id)` 原子摘除——
若摘到本 pending（等待方赢）直接释放；若已被响应线程取走（响应方赢），则有界等待其 deliver 完成，
释放迟到结果后再释放 pending，杜绝泄漏与双重释放。Java 的守护线程调度器在 `close()` 时 `shutdownNow`。

### 8.2 JSON 递归深度上限（64 层）

- **c/json.c**：`#define JSON_MAX_DEPTH 64`，Parser 增加 `depth` 计数，`{`/`[` 入层前校验，
  超限返回 NULL → RPC 层反序列化失败 → 回 `Invalid request payload` 错误响应（复用 B5 路径），进程不崩溃。
- **cpp/include/ipc/json.hpp**：`Parser::MAX_DEPTH = 64`，超限抛 `std::runtime_error("json: max depth exceeded")`
  → `handleMessage` catch → 回错误响应，读循环继续。
- 其余语言解析器为迭代/托管实现或已有深度保护，本轮未改。

### 8.3 新增测试

- 七语言均注册 `slow` handler（睡 500ms），用例：显式 100ms 超时 → 断言 `ETIMEDOUT` →
  等迟到响应到达后再发正常请求，验证 pending 清理与链路无损；同时断言默认超时为 15s。
- C/C++ 另加 JSON 深度用例：200 层嵌套 parse 失败（C 返回 NULL / C++ 抛异常）、60 层成功；
  e2e：客户端发 200 层嵌套参数的 `echo` 帧 → 服务端回 `Invalid request payload` → 后续 `add` 正常，服务不崩溃。

### 8.4 复测结果（Windows / 回环 TCP）

| 语言 | 回环测试（含新增超时/深度用例） |
|---|---|
| Python | 16/16 |
| C# | 16/16 |
| Java | 17/17 |
| Go | `go test -count=1 -race` 全部 ok（含 TestRPCTimeout） |
| Rust | 8/8 |
| C++ | 21/21 |
| C | 19/19（`-Wall` 零警告） |

互操作冒烟：Python↔Node 12/12 通过（线格式零回归）；`examples/csharp` 编译 0 警告 0 错误（API 向后兼容）。

### 8.5 仍未覆盖的 P0/P1 项

~~心跳/keepalive（半开连接检测）、3s 重同步超时断链 + `ERESYNC`、鉴权与版本协商仍缺~~
**已在下一轮（§九）全部补齐**：协议升级 1.1，八语言统一落地心跳 PING/PONG、可选 HELLO 握手（鉴权 + 版本协商）、3s 重同步超时断链。

## 九、协议 1.1：心跳、HELLO 握手与重同步超时（本轮新增）

补齐 §8.5 所列三项 P0/P1。`PROTOCOL.md` 升级 **1.1** 作为事实标准；**18B 消息头与帧线格式不变**，新增消息类型 3–6，旧调用方源码兼容（心跳/握手默认不强制）。

### 9.1 规格要点（PROTOCOL.md §4.5 / §8.1 / §8.2）

- **消息类型**：`0=REQUEST,1=RESPONSE,2=EVENT,3=PING,4=PONG,5=HELLO,6=HELLO_ACK`。
- **心跳（§8.1）**：默认 interval 5000ms / timeout 15000ms（interval≤0 关闭）。任何入站消息刷新 `lastReceived`；静默≥timeout 以 **`EHEARTBEAT`** 结算全部 pending 并断链；静默≥interval 发 PING；收到 PING 立即回 PONG（回显 id）。PING/PONG 为 TEXT 空载荷，不受鉴权门禁。
- **HELLO 握手（§8.2）**：客户端发 `{"version":1,"token":?}`，服务端回 `{"version":1,"ok":true}` 或 `{"error":...}`；失败后服务端断链。`requiredToken` 非空时启用门禁：未鉴权 REQUEST 回 `{"error":"Unauthorized"}` 后断链，EVENT 丢弃。ACK error → **`EAUTH`**，版本不符 → **`EVERSION`**。
- **重同步超时（§4.5）**：`syncTimeout` 默认 3000ms（≤0 关闭）。进协议模式启动计时，RESET_CONFIRM/close 以世代号作废旧计时；超时以 **`ERESYNC_TIMEOUT`** 断链。协议模式下业务 send 门控等待，错误包/协议消息旁路门控（防自锁）；流错误（含 `ERESYNC`、CRC 失败）结算全部 pending 并透传 code。

### 9.2 各语言实现手法

| 语言 | 门控/计时原语 | 心跳载体 | 错误码载体 |
|---|---|---|---|
| JS | 发送队列 + 重同步 Promise | RpcProtocol 定时器 | `RpcError.code` / stream error code |
| Python | `asyncio` 门控 + 计时任务 | asyncio task | `RpcError.code` |
| C# | 门控 + `CancellationToken` 计时 | `Task` 心跳循环 | `RpcException.Code` |
| Java | `wait/notify` + `syncGeneration` | 守护线程节拍 | `RpcException.getCode()` |
| Go | `sync.Cond` + `AtomicU64` 世代 | goroutine 心跳 | `RpcError.Code` |
| Rust | `Condvar` + `AtomicU64` + detached 线程（`Weak` 防悬垂） | 心跳线程 | `RpcError::Coded{code}` |
| C++ | `condition_variable` + atomic 世代 + `enable_shared_from_this`/`weak_ptr` | 心跳线程 | `RpcException.code` / `StreamError` |
| C | `pthread_mutex`/`pthread_cond_timedwait` + 世代号 + `sync_watchers` 计数防 use-after-free | pthread 心跳线程 | `RpcResult.code` / `ms_on_error` code 参 |

C 的 `ms_destroy` 等读线程与全部重同步计时线程退出后再释放；`rpc_destroy` 先 close 唤醒心跳线程再 join，杜绝悬垂与双重释放。

### 9.3 复测结果（Windows / 回环 TCP）

| 语言 | 回环测试（含 v1.1 心跳/HELLO/重同步超时用例） |
|---|---|
| JS | 13/13（`node test_v11.js`） |
| Python | 27/27 |
| C# | 25/25 |
| Java | 28/28（`-encoding UTF-8` 零警告） |
| Go | `go test -count=1 -race ./...` 全部 ok |
| Rust | 15/15 |
| C++ | 32/32 |
| C | 30/30（重复 3 次稳定，example 编译通过） |

各语言 v1.1 用例统一覆盖：HELLO 成功/错误 token（EAUTH）/未鉴权请求（Unauthorized）/版本不匹配（EVERSION）、心跳保活、心跳超时（EHEARTBEAT 结算 pending + 断链）、重同步超时（ERESYNC_TIMEOUT 断链）。坏帧/静默对端构造：坏帧对端发 `le32(5)+le32(0xDEADBEEF)+"hello"+crc` 后静默；重同步计时 300ms、等待 500ms 断言。

### 9.4 互操作冒烟（线格式零回归）

- Python↔Node：12/12（含 CRC 损坏注入→重同步→RPC 恢复）。
- C 客户端→Node 服务端：11/11；Node 客户端→C 服务端：10/10。
