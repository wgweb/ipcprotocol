# C# 实现的 Native AOT 支持

> 结论：`IpcProtocol` 库与它的协议核心已经可以在 Native AOT 下完整运行；
> 业务载荷使用 `JsonTypeInfo<T>`（源生成）路径，运行时任意 POCO 的反射序列化在 AOT 下会被明确拒绝。
> 线格式一个字节都没变。

## 一、改造内容

| 位置 | 改动 | 原因 |
|---|---|---|
| `JsonPayload.cs`（新增） | 手写 `Utf8JsonWriter` 序列化层 | 协议报文（HELLO / HELLO_ACK / REQUEST / 错误）不再经过 STJ 反射 |
| `RpcProtocol.Serialize` | 改为 `SerializeRaw` + `SerializePayload` | 内部报文直接走已编码字节，零序列化器 |
| `RpcProtocol.HelloAsync` / `HandleHelloAsync` | 改用 `JsonPayload.Hello` / `HelloAck` / `Error` | 去掉匿名类型 |
| 错误响应 | 改用 `JsonPayload.Error` + `SendResponseBytesAsync` | 去掉匿名类型 |
| `SendRequestAsync` | 用 `JsonPayload.Request` 组包 | 去掉匿名类型 |
| `EncodeBinaryRequest` | 参数用 `JsonPayload.Serialize` | 同上 |
| 载荷写入 | 基础类型 / `JsonElement` / `byte[]` / 集合 / `IDictionary` 全部无反射分支 | AOT 下可直接序列化 |
| 任意 POCO | 回退反射，且被 `JsonSerializer.IsReflectionEnabledByDefault` 守卫 | AOT 下抛 `NotSupportedException`，给出明确指引 |
| `Convert<T>` / `SendRequestAsync<T>` | 保留反射版并标注 `[RequiresUnreferencedCode]` / `[RequiresDynamicCode]` | 调用方在 AOT 工程里会收到编译期告警 |
| `IpcProtocol.csproj` | `IsAotCompatible=true`、`InvariantGlobalization=true` | 构建期暴露 IL2026/IL3050，而不是运行时才发现 |

## 二、AOT 安全的新 API

```csharp
// 1) 源生成上下文（AOT 必须）
[JsonSerializable(typeof(AddArgs))]
[JsonSerializable(typeof(AddResult))]
internal sealed partial class SmokeJson : JsonSerializerContext { }

// 2) 把强类型值转成 JsonElement
JsonElement arg = RpcProtocol.Json(new AddArgs(3, 4), SmokeJson.Default.AddArgs);

// 3) 位置参数数组（params 必须是数组，服务端才能按位置取参）
JsonElement parameters = RpcProtocol.Args(arg);

// 4) 请求 + 类型化响应
var sum = await client.SendRequestAsync("add", parameters, SmokeJson.Default.AddResult);

// 5) 服务端类型化读取与返回
server.RegisterMethod<AddResult>("add", args =>
{
    var input = RpcProtocol.Convert<AddArgs>(args[0], SmokeJson.Default.AddArgs)!;
    return new AddResult(input.A + input.B);
}, SmokeJson.Default.AddResult);

// 6) 类型化事件
await server.SendJsonAsync(new AddResult(42), SmokeJson.Default.AddResult);
```

`RpcProtocol.Convert<T>(value)`（不带 `JsonTypeInfo`）与 `SendRequestAsync<T>`（不带 `JsonTypeInfo`）仍然可用，
但它们是反射路径：JIT 下照旧工作，AOT 下不可用（分析器会告警）。

## 三、验证结果（本机 linux-x64，.NET SDK 8.0.428）

```bash
# 1) 库：零 IL2026 / IL3050 告警（分析器已确认生效）
dotnet build csharp/IpcProtocol/IpcProtocol.csproj -c Release \
  -p:WarningsAsErrors=IL2026%3BIL3050

# 2) JIT 回归：25/25 通过
dotnet run -c Release --project csharp/IpcProtocol.Tests

# 3) Native AOT 发布并运行冒烟测试
dotnet publish csharp/IpcProtocol.AotSmoke/IpcProtocol.AotSmoke.csproj \
  -c Release -r linux-x64 -o out/aot-linux64
./out/aot-linux64/IpcProtocol.AotSmoke
```

实测：

| 项 | 结果 |
|---|---|
| 库构建（AOT 分析器开，告警即错误） | 通过，0 个 IL2026 / IL3050 |
| JIT 回环回归 | `ALL 25 C# TESTS PASSED` |
| AOT 冒烟（自动 hello / typed 请求 / JsonElement 回显 / 二进制响应 / 二进制请求 / typed 事件） | 全部通过 |
| AOT 下任意 POCO | 按设计抛 `NotSupportedException`，提示改用 `JsonTypeInfo` |
| `IsReflectionEnabledByDefault` | AOT 产物为 `False` |
| 产物体积（linux-x64，含 STJ 源生成） | 约 3.8 MB（去符号） |
| 整进程耗时（含同样冒烟逻辑） | AOT ≈ 10 ms；JIT ≈ 170 ms |

## 四、限制

1. **业务载荷必须是 AOT 可见类型**：`JsonElement`、`string`、数值、`byte[]`、集合/字典，或由源生成登记的 POCO。
   运行时的任意 POCO 无法在 AOT 下被反射解析，这是设计约束而非缺陷。
2. **平台**：Native AOT 支持 win-x64 / linux-x64 / linux-arm64 / macOS x64+arm64，不支持 32 位 x86；
   Windows 产物需要在 Windows 上发布（Linux 不能交叉编译到 Windows）。
3. **命名管道**：`NamedPipeServerStream` / `NamedPipeClientStream` 在 AOT 下可用；
   Linux 上它们映射到 Unix domain socket，冒烟测试用的就是这条路径。
4. **`Process`（Supervisor）**：属于 AOT 白名单 API，不受影响；`ProcessStartInfo` 的 `UseShellExecute=true` 在 Linux 上不可用，
   原有代码已是 `false`，无需改动。
