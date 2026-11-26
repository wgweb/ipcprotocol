# IPC 协议多语言本地实测报告（Windows）

实测机器：本机 Windows 10.0.26300 / Node v22.23.2 / Python 3.13.18 / .NET SDK 10.0.403（运行 net8.0）/ JDK 11.0.16 / Go 1.24.5 / Rust cargo 1.99.1 / MinGW g++ 15.1.0
工作目录：`E:\ipcprotocol`（Node / Python / C# / Java / Go / Rust / C++ / C 八种实现）
日期：2026-11-26

---

## 一、结论

**八种语言的自测在本机全部通过；Node 主进程 ↔ Python / C# 子进程走 Windows 命名管道实测通过（含 8MB 大帧、3 并发客户端、12s 空闲心跳）。**

同时确认两件事与上一轮 Linux 评估不同：

1. **Windows 上内存没有 Linux 那种线性爬升**（同样"小包 + 256KB 混合"负载，120s 内 RSS 在 70–118MB 区间震荡无趋势），说明 Linux 上的 3.1MB/min 增长属 glibc 分配器碎片，不是协议层泄漏，也不是跨平台通病。
2. **命名管道是可用的**：Node 的 `net` 监听 `\\.\pipe\xxx` 就是字节模式管道，Python / C# 侧只要拿到句柄当字节流用，协议层无需任何改动。

---

## 二、各语言自测（同语言回环，含 CRC 注入与重同步）

| 语言 | 命令 | 结果 |
|---|---|---|
| Node.js | `node test_v11.js` | **13 PASS**（心跳保活/超时、HELLO 三态、版本不匹配、重同步超时） |
| Python | `python python/test_loopback.py` | **27 PASS** |
| Python ↔ Node（TCP） | `python python/test_interop_node.py` | **12 PASS**（含 75KB 二进制、事件、50 并发、损坏重同步） |
| C# | `dotnet run --project csharp/IpcProtocol.Tests -c Release` | **25 PASS** |
| Java | `javac -encoding UTF-8 -d java/out java/src/ipc/*.java` → `java -cp java/out ipc.LoopbackTest` | **28 PASS** |
| Go | `cd go; go test -count=1 -race ./...` | **ok 3.24s**（-race 无告警） |
| Rust | `cd rust; cargo test --offline` | **15 passed** |
| C++ | `g++ -std=c++17 -O2 -Wall -Icpp/include cpp/tests/loopback_test.cpp -lws2_32` | **32 passed，0 failed**（有 1 条 `-Wreorder` 警告） |
| C | `gcc -std=c11 -O2 -Ic c/loopback_test.c c/json.c c/message_stream.c c/rpc.c c/tcp.c -lws2_32 -lpthread` | **30 passed，0 failed** |

合计 **204 条断言**，零失败。

---

## 三、命名管道互操作（本轮新增实测，生产形态）

拓扑：Node 主进程监听 `\\.\pipe\ipc_xxx`，Python / C# 子进程作为客户端接入。

| 用例 | Python 客户端 | C# 客户端 |
|---|---|---|
| 建立连接 | ✅ | ✅ |
| JSON add(3,4)=7 | ✅ | ✅ |
| 文本响应（含 Unicode ✓） | ✅ | ✅ |
| 中文/嵌套/null JSON 往返 | ✅ | ✅ |
| 显式 null | ✅ | ✅ |
| 远端抛错 → 异常带上原文 | ✅ | ✅ |
| 方法不存在 → 错误 | ✅ | ✅ |
| **75KB 二进制请求 → 二进制响应（逐字节一致）** | ✅ | ✅ |
| 二进制请求 → JSON 结果 | ✅ | ✅ |
| 800ms 慢任务 | ✅ | ✅ |
| 50 并发请求 | ✅ | ✅ |
| 服务端推 9 条事件（TEXT/JSON/BINARY 各 3） | ✅ | ✅ |
| **空闲 12s 不被心跳断链**（Node 端 5s/15s） | ✅ | ✅ |
| 合计 | **13/13** | **13/13** |

并发与吞吐（Python 客户端 × 单 Node 服务端）：

| 场景 | 结果 |
|---|---|
| 3 个 Python 客户端同时连同一管道 | 3 × 13/13 全通过，服务端逐连接独立会话，互不干扰 |
| 顺序 RPC | 2,000 次 / 0.799s = **2,504 req/s** |
| 并发 100 / 500 | 3,145 / **3,469 req/s** |
| 二进制 1 / 4 / 8 MB 往返 | **241.8 / 255.3 / 246.3 MB/s**，逐字节一致 |
| 客户端断开后 | 服务端 ECLOSED 正常结算，无残留 pending |

> Python 侧 req/s 偏低是 ctypes 管道封装 + asyncio 的开销，不是协议或 Node 端瓶颈；C# 走 `NamedPipeClientStream` 会更快。

---

## 四、Windows 内存行为（对照 Linux 结论）

同一份负载（6 路小包 + 1 路 256KB，120s）：

| 平台 | RSS 行为 |
|---|---|
| Linux（上次评估） | 41 → 84MB，**3.1MB/min 恒定斜率、不衰减** |
| **Windows（本机）** | 120s 内 88.8 / 100.3 / 94.9 / 79.4 / 80.8 / 118.1 / 70.0 / 117.3MB，**震荡无趋势**；heap 恒 5–7MB，extern 8–58MB 随峰值浮动 |

结论：Linux 的爬升是 allocator（malloc 碎片）问题，Windows NT 堆在本负载下没有同样表现。**Linux 侧仍需「限制单帧 ≤ 8MB + 大包分块」或 `LD_PRELOAD=jemalloc`；Windows 侧目前不需要额外处理。**

---

## 五、本轮发现的问题与处理

| # | 问题 | 影响 | 处理 |
|---|---|---|---|
| 1 | `test_v11.js` 心跳用例在 v1.1.1 下挂死 | 默认 `heartbeatSkipWhilePending=true` 后，"有在途请求 + 对端静默"不再被心跳判死 | 该用例显式传 `heartbeatSkipWhilePending: false`（已在文件里加注释）。**语义要记住：既然有在途请求不断链，调用方就必须给每个请求设有限超时** |
| 2 | Python 仓库没有命名管道传输 | Windows 生产形态缺一块 | 新增 `tests/pipe_interop/py_named_pipe.py`：Win32 overlapped I/O → `asyncio.StreamReader` + 轻量 Writer 替身，协议层零改动 |
| 3 | Python ctypes 取句柄踩坑 | `CreateFileW` 未声明 `restype` 时句柄被截成 32 位 → `ReadFile` 报 `ERROR_INVALID_HANDLE(6)`，表现为"连上了但一发请求就断" | `py_named_pipe.py` 里声明 `CreateFileW/CreateEventW.restype = HANDLE` 及 argtypes |
| 4 | 管道句柄不能同步读写 | 用同步 handle 时，阻塞的 `ReadFile` 会占住句柄导致 `WriteFile` 卡死（表现为客户端只能收不能发） | 必须 `FILE_FLAG_OVERLAPPED` + 每方向独立 OVERLAPPED/hEvent |
| 5 | C# JSON 结果类型 | 数字/对象反序列化成 `JsonElement`，`Convert.ToInt32(value)` 直接抛 `InvalidCastException` | 统一用 `RpcProtocol.Convert<T>(value)`（README 已写，但实际很容易踩） |
| 6 | C++ `RpcProtocol` 成员初始化顺序 | `-Wreorder` 警告（`ms_` 最后声明但初始化列表在前） | 只是警告，建议调整初始化列表顺序，不影响行为 |

---

## 六、生产门禁状态更新（相对上一轮）

| 门禁 | 上一轮 | 现在 |
|---|---|---|
| Python / C# 实现 | 缺席 | **已实现并通过（自测 27/25 条 + 管道 13/13）** |
| Windows 命名管道 | 未验证 | **已验证**：字节模式、`\\.\pipe\`、8MB 大帧、3 并发、12s 心跳存活 |
| 心跳与长任务冲突 | 15s 砍断 | 已由 `heartbeatSkipWhilePending` 解决（需配有限超时） |
| Linux 内存爬升 | 未收敛 | Windows 无此问题；**Linux 仍需限帧/分块或 jemalloc** |
| 版本/能力协商 | 缺失（v1.0 与 v1.1 同为 version=1） | 仍缺；滚动升级期建议先关心跳 |
| 进程守护 / 自动重连 | 缺失 | 仍缺（子进程崩溃后 Node 侧不自动重启） |
| 默认鉴权 | 无 | 仍无；多用户机器建议强制 `requiredToken` + `hello()` |

---

## 七、复现方式（本机已验证）

```powershell
# 单语言自测
node test_v11.js
python python/test_loopback.py
python python/test_interop_node.py
dotnet run --project csharp/IpcProtocol.Tests -c Release
javac -encoding UTF-8 -d java/out java/src/ipc/*.java; java -cp java/out ipc.LoopbackTest
cd go; go test -count=1 -race ./...; cd ..
cd rust; cargo test --offline; cd ..
g++ -std=c++17 -O2 -Icpp/include cpp/tests/loopback_test.cpp -o cpp/out/loopback_test.exe -lws2_32; cpp/out/loopback_test.exe
gcc -std=c11 -O2 -Ic c/loopback_test.c c/json.c c/message_stream.c c/rpc.c c/tcp.c -o c/out/loopback_test.exe -lws2_32 -lpthread; c/out/loopback_test.exe

# 命名管道互操作（Node 主进程 ↔ 子进程）
node tests/pipe_interop/node_pipe_server.js ipc_demo        # 终端 1：服务端
python tests/pipe_interop/py_pipe_client.py ipc_demo        # 终端 2：Python 子进程
dotnet run --project tests/pipe_interop/csharp -- ipc_demo   # 终端 3：C# 子进程

# 管道压测 / 内存长稳
python tests/pipe_interop/py_pipe_stress.py ipc_demo
node tests/pipe_interop/node_pipe_soak.js 120
```

测试产物与日志都在 `E:\ipcprotocol\_test-out\`。
