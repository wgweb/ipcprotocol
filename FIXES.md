# 修复说明（FIXES）

**线格式零改动**：帧头 / CRC32 / 错误包 / 协议模式消息与 `PROTOCOL.md` 完全一致，Python、C# 等实现按文档写就行，不需要跟着改。

已验证：
- `crc32("hello") = 0x3610A686`，与文档校验向量一致
- 原版实现 ↔ 修复版实现互操作：普通 RPC 500/500，2MB 二进制正确；反方向 200/200

---

## 一、MessageStream.js（重写了收发内核）

| # | 原问题 | 修复方式 |
|---|---|---|
| 1 | 只监听 `data`，断开时静默 | 补 `close` / `end` / `error` → `emit('error', Error with code='ECLOSED')` |
| 2 | `Buffer.concat` 累积，O(n²) | 改为 **Buffer 队列 + 头游标**（`_q` / `_qHead` / `buffered`），单块内零拷贝，跨块只复制一次 |
| 3 | 长度头不校验 | `length === 0 \|\| length > maxBufferSize` → 报 `BAD_LENGTH` + 错误包 + 重同步 |
| 4 | 发送忽略背压、多片写 | 单一串行发送队列；>64KB 分片写并 `await drain`；单次写不再嵌套 `cork/uncork` |
| 5 | 错误包/协议消息绕过队列，和正常帧竞争 | 所有写出（数据 / 错误包 / 协议消息）统一走同一队列，只是错误包跳过同步闸门 |
| 6 | `destroy()` 里 `socket.end('EOF')` 往帧流写 3 字节 ASCII | 改为 `socket.destroy()`，不再污染对端字节流 |
| 7 | 重同步可能永久卡死 | 进入协议模式后启动 3s 计时（`syncTimeout` 可配），超时未收到 `RESET_CONFIRM` → 抛 `ERESYNC_TIMEOUT` 并断链，由上层重连 |
| 8 | 收到错误包一侧静默丢弃在途请求 | 收到错误包 → `emit('error', code='ERESYNC')`，上层据此拒绝所有 pending |
| 9 | 协议模式丢弃时整段清空，可能把 `RESET_CONFIRM` 一起丢掉 | 改为**扫描 `0xFFFFFFFF` 头**，只丢弃头部之前的垃圾，保留可能被截断的 3 字节 |
| 10 | `emit('error', 字符串)` 在无监听时 crash | 统一 `Error` + `code`；无监听器时只 `console.error`，不再抛 `Unhandled error` |
| 11 | 发送期间进入重同步，帧被对端丢弃 | 发送前 `await` 同步闸门（`_syncGate`），同步完成或超时后才继续 |

新增只读状态（调试/测试用）：`buffered`、`expectedLength`、`protocolMode`、`getStatus()`。

---

## 二、RpcProtocol.js

| # | 原问题 | 修复方式 |
|---|---|---|
| 1 | `sendRequest` 无超时，断线后永久 pending | `sendRequest(method, params, to, timeoutMs = 15000)`；传 `0` 关闭超时 |
| 2 | 发送失败时 pending 不清理 | `_sendAndTrack` 里 `sendRaw` 失败 → 删除 pending 并以 `ESEND` 拒绝 |
| 3 | reject 值是字符串 | 统一 `RpcError`（`name/code/remote/data`）；远程错误 `code='EREMOTE'` |
| 4 | `deserialize` 用正则还原 uuid，短帧产生畸形 id | 先校验 `length >= 18`；uuid 按 8-4-4-4-12 切分；非法 JSON → `EMALFORMED` |
| 5 | 畸形请求只打日志，调用方永久等待 | 能解析出 id 时回一个 `{error:'Invalid request payload'}` 响应，让调用方立即失败 |
| 6 | `destroy()` 非幂等、`pendingRequests` 被置 null 后仍被引用 | 加 `_destroyed` 幂等；先 reject 全部 pending 再解绑监听器；`handleStreamError` 判空 |
| 7 | 新增事件监听器泄漏 | 保存 `_onMessage/_onStreamError/_onSync` 引用，destroy 时精确 `off` |
| 8 | `error` 事件无监听直接崩进程 | `_safeEmit`：无监听器时打日志，不抛 |

新增：`export class RpcError`、`syncComplete` 事件（重同步完成后触发）。

---

## 三、nativeService.js

| # | 原问题 | 修复方式 |
|---|---|---|
| 1 | 第二个客户端直接顶掉现有 `rpcProtocol` | `isConnected` 时新连接直接 `socket.destroy()`，原链路不受影响 |
| 2 | `sendRequest/sendText/...` 在未连接时 `return undefined` | 全部返回 `Promise.reject(RpcError)`，`await` 能拿到错误而不是静默 `undefined` |
| 3 | 构造函数里 `startServer()` 抛异常会让实例创建失败 | `startServer` 内部 try/catch + 事件上报；Unix socket 先 `unlink` 残留文件 |
| 4 | `stop()` 无锁，重复调用注册多个监听/定时器 | 加 `_stopping` 锁，`finish` 只执行一次 |
| 5 | `waitForConnection` 超时后监听器不清理 | 每次等待登记 waiter，超时/成功统一 `cleanup` |
| 6 | 关闭后不删 sock 文件 | `server.close()` 回调里 `unlink`（Unix） |
| 7 | `stdio` 写死 pipe，子进程日志多会阻塞 | 新增 `stdio` 选项，可传 `'ignore'` 或 `['ignore','pipe','pipe']` |
| 8 | 无 error 兜底 | 构造时挂默认 `error` 监听（打印 + 继续运行） |

---

## 四、行为约定变更（对接 Python/C# 时需要知道）

1. **重同步会丢弃在途请求**：任一端检测到 CRC 错误时，两端在途请求都会以 `ERESYNC` 拒绝，业务需要重试。这是有意设计——宁可让调用方知道失败，也不能让它永久挂起。
2. **重同步 3s 不成功就断链**：`syncTimeout` 默认 3000ms。跨语言实现只要能正确回应 `RESET_CONFIRM`，就不会触发。
3. **默认 RPC 超时 15s**：长耗时方法请显式传更大的 `timeoutMs`，或传 `0` 关闭。
4. 错误对象一律带 `code`，建议业务按 `code`（`ETIMEDOUT` / `ERESYNC` / `ECLOSED` / `EREMOTE` / `ESEND` / `ETOOBIG`）分支，而不是匹配 message 文本。

---

## 五、尚未覆盖（需要你在目标平台验证）

- Windows 命名管道：本地无 Windows 环境，Linux UDS 上验证通过；命名管道是字节流，语义一致，但 `setNoDelay` 之类调用已用可选链兼容。
- Python / C# 实现：尚未编写，按 `PROTOCOL.md` + 本文第四节约定实现即可。
