# IPC 协议栈 P1 补齐验证报告

- 日期：2026-11-26
- 环境：Windows（本机），Node v22.23.2 / Python 3.13.18 / .NET 10.0.403 / JDK 11.0.16 / Go 1.24.5 / Rust 1.99.1 / MinGW gcc 15.1.0
- 范围：① 五语言（Java/Go/Rust/C++/C）能力协商；② 多客户端会话；③ token 热轮换
- 结论：**P1 三项全部关闭，全量回归 100% 通过，线格式零改动**

---

## 一、交付清单

| 文件 | 变更 |
| --- | --- |
| `RpcProtocol.js` | `requiredTokens: Set` + `setRequiredTokens()`；`_authenticated` 初值随 token 集合为空而放行 |
| `nativeService.js` | `maxClients`/`clients`/`_primaryId`/多客户端 API/`rotateToken`/`revokeToken`；状态补 `clients`、`primaryClient`、`acceptedTokens` |
| `java/src/ipc/RpcProtocol.java` | caps/impl/peer/server、`disableHeartbeat`、5 次未鉴权、`IPC_TOKEN` 兜底 |
| `go/rpc_protocol.go` | 同上（`hbOnce sync.Once`、`HeartbeatDisabledReason()`） |
| `rust/src/rpc_protocol.rs` | 同上（`unauthorized_hits`、300ms detach close） |
| `cpp/include/ipc/rpc_protocol.hpp` | `State` 增 caps 字段、`capsJson()`/`readCaps()`/`hasPing()`、300ms detach close |
| `c/rpc.c` + `c/rpc.h` | caps 字段与访问器、`caps_csv`/`caps_to_json`、`IPC_TOKEN` 兜底、立即 close |
| `tests/v112/test_multiclient_rotation.js` | 新增：多客户端 + 轮换 21 断言 |
| `tests/v112/run_all.ps1` | 扩为 5 组 + 五语言自测 |
| `PROTOCOL.md` | 新增 §8.5（五语言 caps 矩阵 / 多客户端语义 / 轮换语义 / 实现进度 / 回归表） |

---

## 二、关键设计决策

**1）能力协商复用 HELLO 体，不新增帧类型。** 老实现对多余字段忽略，新实现缺失 `caps` 时按"旧实现"处理，保持零风险升级。

**2）"对端不会心跳"要比"本端会心跳"更重要。** 只要对端声明了 `caps` 且不含 `ping`，本端就停发心跳并停用静默超时，避免 15s 误杀。五语言均实现且原因可读。

**3）C 语言的延迟断链是坑，必须例外。** 学 Node 做"300ms 后 detached 线程 close"，会在调用方 `rpc_destroy()` 之后 use-after-free（实测 `version mismatch EVERSION` 后 0xC0000005）。改为立即 `rpc_close()`：`ms_send` 是同步写，ack 已进内核缓冲，不丢包。其余语言对象所有权/生命周期可控，保留延迟 close。

**4）多客户端的"主会话"是稳定路由锚。** 默认调用永远打主会话，主会话断开自动切到下一个并 emit `primaryChanged`；只有全部清空才 emit `disconnect`。这样上层业务代码不需要关心连接数量变化。

**5）轮换要让旧 token 有"体面退场"。** `requiredTokens` 是集合而非单值：轮换瞬间新旧并存，旧 token 在宽限期（默认 5 分钟）内仍可完成握手；已握手会话的鉴权位粘性，不受轮换影响；到期自动吊销，未握手的老客户端被 `EAUTH` 挡住。

**6）客户端与服务端的"方法注册"是双向的。** 主进程 `svc.call.*` 打子进程，子进程 `rpc.sendRequest` 打主进程，两者都需要对端 `registerMethod`。这一点在测试中被验证（双向 'add' 均可执行）。

---

## 三、回归结果

### 3.1 Node 侧（协议功能）

| 套件 | 断言 | 结果 |
| --- | --- | --- |
| `test_auth_caps.js`（鉴权 + 能力协商） | 12 | 12 / 12 |
| `test_restart.js`（守护：真实崩溃 + 退避 + 重连） | 22 | 22 / 22 |
| `test_multiclient_rotation.js`（多客户端 + 轮换） | 21 | 21 / 21 |
| `test_v11.js`（v1.1 心跳/鉴权/重同步基线） | 13 | 13 / 13 |

多客户端用例覆盖：3 会话并发、主会话唯一性、默认路由、`callTo` 精确路由、`broadcastText` 全员送达、主会话断开后自动切换且默认路由指向新主会话。

轮换用例覆盖：新 token 立即生效、宽限期新旧共存、在跑会话零影响、宽限期后旧 token 被吊销（`EAUTH`）、已握手旧会话继续可用。

### 3.2 Python / C#

| 套件 | 断言 | 结果 |
| --- | --- | --- |
| Python 正确 token（继承 `IPC_TOKEN` + 自动 hello） | 3 | 3 / 3 |
| Python 错误 token → `EAUTH` | 1 | 1 / 1 |
| C# 正确 token | 3 | 3 / 3 |
| C# 错误 token → `EAUTH` | 1 | 1 / 1 |

### 3.3 五语言自测（含新增 caps 逻辑）

| 语言 | 套件 | 结果 |
| --- | --- | --- |
| Java | `ipc.LoopbackTest` | 28 / 28 |
| Go | `go test ./...` | ok（2.37s） |
| Rust | `cargo test` | 15 / 15 |
| C++ | `loopback_test` | 32 / 32 |
| C | `loopback_test` | 30 / 30 |

各语言用例已包含：`hello ok`、`hello wrong token EAUTH`、`unauthorized`（5 次容错）、`version mismatch EVERSION`、心跳 keepalive/timeout/close、resync 系列。

### 3.4 一键复跑

```powershell
pwsh -NoProfile -File E:\ipcprotocol\tests\v112\run_all.ps1
```

输出 `v1.1.2 + P1 全部回归通过`。日志落盘 `E:\ipcprotocol\_test-out\p1_all.log`。

---

## 四、门禁状态

| 级别 | 项 | 状态 |
| --- | --- | --- |
| P0-3 | 能力协商缺失 | ✅ 关闭（8 语言全覆盖） |
| P0-5 | 无守护/无重连 | ✅ 关闭（Node 侧退避 + 事件 + 优雅停止） |
| P0-6 | 默认无鉴权 | ✅ 关闭（默认开、env 下发、失败 300ms 断链、5 次容错） |
| P1 | 其余语言 caps | ✅ 关闭（Java/Go/Rust/C++/C） |
| P1 | 多客户端会话 | ✅ 关闭（上限 8、主会话切换、精确路由、广播） |
| P1 | token 热轮换 | ✅ 关闭（集合式接受、宽限期、粘性鉴权） |
| P2 | 五语言自动 hello 封装 | ⏳ 待办（不影响安全，纯体验） |
| P2 | 宿主侧守护编排（Python/C#/Java… 由外层拉起） | ⏳ 待办 |
| P2 | Linux/macOS 跨平台复验 | ⏳ 待办（协议层无平台假设，历史 Linux 压测已通过） |

---

## 五、上线前建议

1. 生产默认保持 `requireAuth: true`；token 由主进程下发到子进程 env，不要硬编码进仓库。
2. 长跑服务建议 `rotateToken({ graceMs })` 挂在定时器上（如每 24h），宽限期不少于一次完整的子进程重启周期，避免重启中的子进程拿旧 token 被拒。
3. 子进程数量可控时把 `maxClients` 调到实际规模；超限连接会被直接 destroy，不会挤掉主会话。
4. C 语言客户端务必在首个业务请求前显式 `rpc_hello()`（P2 自动 hello 尚未覆盖）。
5. 若与老版本子进程混跑，临时关闭鉴权或给每个老进程单独放行，升级完成后再全量开启。
