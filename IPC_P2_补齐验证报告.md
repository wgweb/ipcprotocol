# IPC 协议栈 P2 补齐验证报告

- 日期：2026-11-26
- 范围：① 五语言自动 hello；② 宿主侧守护（Python/C#/Java）；③ Windows + Linux 跨平台复验
- 结论：**P2 三项全部关闭；Windows 全量回归与 Linux 主要链路回归均通过；线格式零改动**

---

## 一、交付清单

| 文件 | 变更 |
| --- | --- |
| `java/src/ipc/RpcProtocol.java` | 新增 `ensureAuth()` / `isAuthSent()`；`sendRequest*` 前自动握手；鉴权语义拆分 |
| `java/src/ipc/Supervisor.java` | 新增：Java 宿主侧守护（退避重启 / 优雅停止 / 事件） |
| `java/src/ipc/SupervisorTest.java` | 新增：Java 守护 8 断言 |
| `java/src/ipc/LoopbackTest.java` | 新增自动握手 4 用例（首请求 / 幂等 / 并发 / 错误 token） |
| `go/rpc_protocol.go` | 新增 `ensureAuth()` / `AuthSent()`；`SendRequest*` 前自动握手；鉴权语义拆分 |
| `go/rpc_test.go` | 新增 `TestAutoHello{FirstRequest,Concurrent,WrongToken}` |
| `rust/src/rpc_protocol.rs` | 新增 `ensure_auth()` / `auth_sent()`；发送前自动握手；鉴权语义拆分 |
| `rust/tests/loopback.rs` | 新增 3 个自动握手用例 |
| `cpp/include/ipc/rpc_protocol.hpp` | 新增 `ensureAuth()` / `authSent()`；发送前自动握手；鉴权语义拆分 |
| `cpp/include/ipc/tcp.hpp` | 新增 POSIX socket 分支（Linux/macOS 可编译） |
| `cpp/tests/loopback_test.cpp` | 新增自动握手 4 用例 |
| `c/rpc.c` / `c/rpc.h` | 新增 `rpc_ensure_auth()` / `rpc_auth_sent()`；发送前自动握手；鉴权语义拆分 |
| `c/tcp.c` | 重写为 Windows Winsock / POSIX 双分支 |
| `c/rpc.c`、`c/message_stream.c` | 补 `_POSIX_C_SOURCE 200809L`（`clock_gettime`） |
| `c/loopback_test.c` | `Sleep()` 跨平台映射 + 自动握手 4 用例 |
| `python/supervisor.py` | 新增：Python 宿主侧守护 |
| `csharp/IpcProtocol/Supervisor.cs` | 新增：C# 宿主侧守护 |
| `tests/v112/test_supervisor.py` | 新增：Python 守护 8 断言 |
| `tests/v112/csharp/SupervisorTest.csproj` + `SupervisorTest.cs` | 新增：C# 守护 8 断言 |
| `tests/v112/run_all.ps1` | 扩为 5 组 + 三语言守护 + 五语言自测 |
| `PROTOCOL.md` | 新增 §8.6（自动握手矩阵 / 宿主守护 / 跨平台 / 回归表） |

---

## 二、这轮修掉的一个真 bug：鉴权语义混淆

五个语言（Java/Go/Rust/C++/C）原本都是：

```text
authenticated = (requiredToken == null)
```

客户端构造时 `requiredToken == null`，于是**自己把自己标成"已鉴权"**。后果有两层：

1. 新增的 `ensureAuth()` 看到 `authenticated == true` 直接短路，永远不会发 HELLO；
2. 业务请求裸发到要求鉴权的服务端 → 服务端回 `Unauthorized`。

实测表现：Java 首个自动握手用例直接抛 `Unauthorized`（服务端拒绝），根本走不到握手。

修法是把一个布尔拆成两个概念：

| 概念 | 含义 | 谁用 |
| --- | --- | --- |
| `authenticated` | 对端已通过我的校验 | 仅服务端角色（`required_token` 非空）用它拒绝请求 |
| `authSent` | 我已经握手完成 | 客户端侧 `ensureAuth()` 的幂等标记 |

守门条件也从 `if (!authenticated)` 改成 `if (required_token != null && !authenticated)`，修复后客户端不会再拒绝服务端下发的请求（双向调用场景必需）。

---

## 三、自动握手的实现要点

- **只在客户端角色生效**：服务端（`required_token` 非空）不自动握手；无 token 也不握手（旧行为不变）。
- **并发只握手一次**：Java `synchronized` + 共享 `CompletableFuture`；Go/Rust/C++/C 用互斥锁 + 完成标记，锁内二次检查。Go/Rust/C++ 已有 8 线程并发用例验证。
- **失败原样抛出**：`EAUTH` / `EVERSION` 直接返回给调用方，不发业务请求，避免"错误 token 触发 5 次未鉴权断链"的雪崩。
- 不改变显式 `hello()` 语义：既有的 `hello ok` / `hello wrong token` 用例全部保留并通过。

---

## 四、宿主侧守护

三份实现（Python / C# / Java）严格对齐 Node `nativeService` 的策略：

| 策略 | 值 |
| --- | --- |
| 初始退避 | 500ms |
| 退避增长 | 翻倍 |
| 退避上限 | 30s |
| 计数清零 | 进程稳定运行 `resetAfterMs`（默认 30s）后 |
| 放弃条件 | `maxRestarts` 用尽后只回调 `onGiveUp`，不再重启 |
| 显式停止 | `stop()` / `StopAsync()`：不触发 crash 事件、不再重启 |

验证覆盖：真实 `sys.exit(3)` 连崩两次 → 退避增长（150→300ms）→ 到达 `maxRestarts` 放弃；长驻子进程 `stop()` 立刻结束且**不产生** crash 事件。

> 注意：三份守护都只负责"进程活着"，不负责"管道/会话重连"；连接重建仍由各语言的 `RpcProtocol` 与新连接完成（管道名/端口不变即自动可用）。

---

## 五、跨平台

为了在 Linux 上真正跑起来，补了两处平台缺口：

1. **C 与 C++ 的 TCP 传输原本只有 Winsock 实现**（整个文件被 `#ifdef _WIN32` 包着，Linux 上根本没有可用传输层）。本轮增加 POSIX 分支：`socket/bind/listen/accept/connect` 语义一致，差异点用宏收敛（`IPC_INVALID_SOCK`、`IPC_SEND_FLAGS`(MSG_NOSIGNAL)、`IPC_SHUT_BOTH`、`IPC_SOCK_ERR`）。
2. **C 的 `clock_gettime` / `nanosleep` 在 `-std=c11` 下不可见**：补 `_POSIX_C_SOURCE 200809L`；测试里的 `Sleep(ms)` 在非 Windows 下映射为 `nanosleep`，避免 `usleep` 的隐式声明告警。

改完后同一份源码在两个平台都编译通过、断言全绿（Windows 侧已重新编译验证，未引入回归）。

---

## 六、回归结果

### 6.1 Windows（本机，全量）

| 套件 | 结果 |
| --- | --- |
| `test_v11.js`（v1.1 基线） | 13 / 13 |
| `test_auth_caps.js`（鉴权 + 能力协商） | 12 / 12 |
| `test_restart.js`（守护） | 22 / 22 |
| `test_multiclient_rotation.js`（多客户端 + 轮换） | 21 / 21 |
| Python 鉴权（good / bad） | 3 + 1 |
| C# 鉴权（good / bad） | 3 + 1 |
| Java LoopbackTest（含自动握手 4 例） | 32 / 32 |
| Go `go test` | ok（2.37s） |
| Rust `cargo test` | 18 / 18 |
| C++ loopback（含自动握手 4 例） | 36 / 36 |
| C loopback（含自动握手 4 例） | 34 / 34 |
| 守护：Python / C# / Java | 8 / 8 / 8 |

一键脚本：`pwsh -NoProfile -File tests\v112\run_all.ps1` → 输出 `v1.1.2 + P1 + P2 全部回归通过`。

### 6.2 Linux（云主机 Ubuntu 22.04，Node 22 / Python 3.12 / GCC 11 / Go 1.23）

| 套件 | 结果 |
| --- | --- |
| Node `test_v11.js` | 13 / 13 |
| Node 鉴权 + 能力协商 | 12 / 12 |
| Node 守护（真实崩溃 + 退避） | 22 / 22 |
| Node 多客户端 + token 轮换 | 21 / 21 |
| Python 自测 | 27 / 27 |
| Python 守护 | 8 / 8 |
| Go `go test` | ok（1.83s） |
| C++ loopback（POSIX socket） | 36 / 36 |
| C loopback（POSIX socket） | 34 / 34 |

未执行：Java（云主机只有 JRE，无 `javac`）、Rust（无 `cargo`）。两者的逻辑与平台无关，已在 Windows 侧验证。

---

## 七、门禁状态（最终）

| 级别 | 项 | 状态 |
| --- | --- | --- |
| P0-3 | 能力协商 | ✅ 关闭（8 语言） |
| P0-5 | 进程守护 / 重连 | ✅ 关闭（Node + Python/C#/Java 宿主守护） |
| P0-6 | 默认鉴权 | ✅ 关闭（含自动握手、5 次容错、300ms 断链） |
| P1 | 多语言 caps | ✅ 关闭 |
| P1 | 多客户端会话 | ✅ 关闭 |
| P1 | token 热轮换 | ✅ 关闭 |
| P2 | 自动 hello | ✅ 关闭（Java/Go/Rust/C++/C） |
| P2 | 宿主侧守护编排 | ✅ 关闭（Python/C#/Java） |
| P2 | 跨平台验证 | ✅ 关闭（Windows + Linux；Java/Rust 仅 Windows） |

**剩余可选项（不算门禁）**：

- Python ↔ Node 命名管道互操作脚本仍是 Windows 专用（`CreateFile`/`WaitNamedPipe`）；如需 Linux 互测，把 `py_named_pipe.py` 换成 `open_unix_connection` 即可，工作量约 30 行。
- macOS 未实测（POSIX 分支理论上覆盖，但没有实机）。
- 自动握手目前是"首个发送前拦截"，没有做"连接建立即主动握手"；需要时可在连接回调里直接调 `hello()`。

---

## 八、上线建议（增量）

1. 子进程只要带 `IPC_TOKEN` 启动，无需在业务代码里手动 `hello()`——五语言均已自动完成，且并发安全。
2. 非 Node 宿主请直接用对应语言的 `Supervisor`，不要自己写 restart 循环：稳定期清零策略、优雅停止、放弃语义都已在类里固化。
3. Linux 部署时管道路径是 Unix socket 文件，注意容器/挂载目录的写权限与残留 socket 清理（`nativeService` 关闭时会 unlink）。
4. 跨平台构建脚本按平台分别链接：Windows 加 `-lws2_32`，Linux 加 `-lpthread`（C++/C）。
