# IPC v1.1.2 生产加固：能力协商 / 默认鉴权 / 进程守护

> 环境：Windows 10.0.26300 本机，Node v22.23.2 / Python 3.13.18 / .NET 8
> 结论：三项阻塞项全部实现并通过回归（新增 34 条断言 + 13 条 v1.1 基线 + Python 27 / C# 25 自测 + 管道互操作 26 条）。

## 一、三项加固的实现要点

| 项 | 做法 | 兼容策略 |
| --- | --- | --- |
| 版本/能力协商 | HELLO / HELLO_ACK 增加 `caps: ["ping","binary","event"]` 与 `impl` 字段；双方记录 `peerCapabilities` / `serverCapabilities` | 缺失 `caps` 视为旧实现，行为与 v1.1.1 完全一致 |
| 心跳误杀消除 | 对端声明了 caps 但不含 `ping` → 本端停用心跳与静默超时判定，并 `emit('heartbeatDisabled', reason)`；`heartbeatRequireCapability=true` 可强制"无 ping 不心跳" | 默认宽松，老实现不受影响 |
| 默认鉴权 | `NativeService` 默认 `requireAuth=true`，`randomUUID()` 生成每实例 token → 服务端 `requiredToken` 强校验 → 子进程经 `IPC_TOKEN` 环境变量获得并在首个请求前自动 `hello()` | `requireAuth:false` 或显式 `authToken` 为逃生开关 |
| 进程守护 | `autoRestart=true`，指数退避 500ms→30s，事件 `crash / restarting / restarted / give-up`，管道名不变、子进程自动重连，`stop()` 不触发重启 | `autoRestart:false` 关闭；`maxRestarts` 控制放弃阈值 |

## 二、改动文件

| 文件 | 改动 |
| --- | --- |
| `RpcProtocol.js` | `capabilities` / `implName` / `heartbeatRequireCapability`、`peerCapabilities`、`serverCapabilities`、`heartbeatDisabledReason`、`_disableHeartbeat()`；HELLO/HELLO_ACK 携带 caps；未鉴权 REQUEST 改为**回 `{error:"Unauthorized"}` 且不断链**（连续 5 次才断）；握手失败改为 ack 落地后 300ms 再断链 |
| `nativeService.js` | `requireAuth` / `authToken` / 自动生成 token、`IPC_TOKEN`+`IPC_REQUIRE_AUTH` 下发、`requiredToken` 接进服务端 RpcProtocol；`autoRestart` / `restartDelayMs` / `maxRestartDelayMs` / `maxRestarts` / `restartResetAfterMs`、`_scheduleRestart()`、crash/restarting/restarted/give-up 事件、`getProcessStatus()` 增加 crashes/restarting/restartAttempts/authRequired |
| `python/rpc_protocol.py` | `CAPABILITIES`、`capabilities`/`impl_name`/`heartbeat_require_capability`、`peer_capabilities`/`server_capabilities`、`heartbeat_disabled_reason`、`_disable_heartbeat()`；`auth_token` 自动读 `IPC_TOKEN`；`_ensure_auth()` + `send_request/send_request_binary` 前自动握手；未鉴权策略与 Node 一致；修复 `_authenticated` 语义混淆（客户端改用 `_auth_sent`） |
| `csharp/IpcProtocol/RpcProtocol.cs` | 同上能力字段、`Capabilities`/`ImplName`/`PeerCapabilities`/`ServerCapabilities`/`HeartbeatDisabledReason`；`AuthToken` 自动读 `IPC_TOKEN`；`EnsureAuthAsync()` 接入 `SendRequestAsync` / `SendRequestBinaryWithTimeoutAsync`；未鉴权策略一致 |
| `PROTOCOL.md` | 新增 §8.4（线格式零改动，字段语义 + 升级注意事项 + 实现进度表） |
| `ipc/MessageStream.js`、`ipc/RpcProtocol.js` | 兼容垫片：`nativeService.js` 按 `modules/ipc` 布局引用，转出根目录同一模块实例（无重复类） |
| `test_v11.js` | 鉴权失败断链断言由 `sleep(100)` 调整为 `sleep(600)`（适配新的"ack 先落地再断链"） |

## 三、验证结果

| 套件 | 内容 | 结果 |
| --- | --- | --- |
| `tests/v112/test_auth_caps.js` | 未握手被拒、错 token→EAUTH、正确 token 业务通、能力双向可见、无 ping 对端不误杀（静默 2.8s 仍可用）、旧实现不降级 | 12/12 |
| `tests/v112/test_restart.js` | 默认鉴权生效、token 下发到子进程、真实崩溃→capture crash→300ms 重启→重连→业务恢复、二次崩溃退避 600ms、崩溃计数、stop 不重启、`autoRestart:false` 不重启 | 22/22 |
| `tests/v112/py_auth_client.py` | Python 从 `IPC_TOKEN` 自动握手、能力双向、错 token→EAUTH | 4/4 |
| `tests/v112/csharp/Program.cs` | C# 从 `IPC_TOKEN` 自动握手、能力双向、错 token→EAUTH | 4/4 |
| `test_v11.js` | v1.1 心跳/鉴权/重同步基线 | 13/13 |
| `python/test_loopback.py` | Python 全量自测 | 27/27 |
| `csharp/IpcProtocol.Tests` | C# 全量自测 | 25/25 |
| `tests/pipe_interop`（Py/C# ↔ Node 管道） | 13 + 13 断言（含 75KB 二进制、9 事件、50 并发、12s 空闲心跳） | 26/26 |

一键复跑：`pwsh -File tests/v112/run_all.ps1`

## 四、用法

```js
// 主进程：默认鉴权 + 守护（全部可调）
const svc = new NativeService('pyworker', {
    cmd: 'python', args: ['worker.py'],
    // 鉴权
    requireAuth: true,          // 默认 true；false = 完全不校验
    authToken: null,            // 默认自动 randomUUID，子进程从 IPC_TOKEN 读取
    // 守护
    autoRestart: true,          // 默认 true
    restartDelayMs: 500,
    maxRestartDelayMs: 30000,
    maxRestarts: Infinity,
    restartResetAfterMs: 30000, // 稳定连接多久后清零退避计数
    // 心跳
    heartbeatIntervalMs: 5000,
    heartbeatTimeoutMs: 15000,
    heartbeatRequireCapability: false,
});
svc.on('crash', ({ code }) => {});
svc.on('restarting', ({ attempt, delay }) => {});
svc.on('restarted', ({ attempt }) => {});
svc.on('give-up', ({ attempts }) => {});
svc.start();
await svc.call.add(1, 2);       // 崩溃/重启期间自动挂起等待重连
```

```python
# Python 子进程：什么都不用做，token 自动继承 + 首个请求前自动 hello
rpc = RpcProtocol(MessageStream(reader, writer))   # auth_token 自动取 IPC_TOKEN
result = await rpc.send_request('add', [1, 2])     # 内部先 hello
```

```csharp
// C# 子进程：同理
using var proto = new RpcProtocol(new MessageStream(pipeStream));  // AuthToken 自动取 IPC_TOKEN
var sum = await proto.SendRequestAsync("add", new object?[] { 1, 2 });  // 内部先 hello
```

## 五、升级注意（必须看）

1. **默认鉴权会打断旧链路**：老 Python/C#/Node 子进程若没有 HELLO，升级后第一个请求就 `EREMOTE(Unauthorized)`。上线切换顺序：先升级子进程实现（Python/C# 已内置自动 hello），再打开 `requireAuth`；过渡期可 `requireAuth:false`。
2. **Node 子进程需显式握手**：Node 侧目前不自动 hello（避免与既有用例语义冲突），Node 写子进程时要 `await rpc.hello()`，或用 `authToken` 构造后显式调用一次。
3. **token 生命周期**：token 为进程级、随机、不落盘；重启主进程即换新 token，子进程随之重启重新读取。暂无热轮换，也不支持多客户端共享同一 token 分权。
4. **守护与单客户端门禁**：`startServer()` 仍是单客户端（第二个连接直接 destroy）。子进程若多开需改多路会话。
5. **`give-up` 后不自动拉起**：`maxRestarts` 用尽只发事件，需外层编排（服务管理器/看门狗）介入。
6. **能力协商只在 Node/Python/C# 落地**：Java/Go/Rust/C++/C 未声明 caps，按协议视为旧实现（不触发停心跳逻辑），功能不受影响，但无法享受"无 ping 不误杀"。

## 六、门禁更新

| 编号 | 项 | 状态 |
| --- | --- | --- |
| P0-3 | 版本/能力协商缺失 | ✅ 关闭（HELLO caps + 实现进度表） |
| P0-5 | 进程守护/断线重连缺失 | ✅ 关闭（autoRestart + 退避 + 重连 + 事件） |
| P0-6 | 默认无鉴权 | ✅ 关闭（默认 token + 环境变量下发 + 自动 hello） |
| P1 | 其余语言补齐 caps / 守护 | 开放（不影响现有互操作） |
| P1 | token 热轮换、多客户端会话 | 开放 |
