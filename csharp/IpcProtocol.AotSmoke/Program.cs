// Native AOT 冒烟测试：验证在反射被禁用（IsReflectionEnabledByDefault == false）时，
// 协议核心与 JsonTypeInfo 路径仍能完成 hello / JSON 请求 / 二进制请求 / 事件。

using System.Diagnostics;
using System.IO.Pipes;
using System.Text.Json;
using IpcProtocol;
using IpcProtocol.AotSmoke;

var ctx = SmokeJson.Default;
var failures = 0;

void Check(string name, bool ok)
{
    Console.WriteLine($"  {(ok ? "ok" : "FAIL")}: {name}");
    if (!ok) failures++;
}

var pipeName = $"aot_smoke_{Guid.NewGuid():N}";
using var serverStream = new NamedPipeServerStream(pipeName, PipeDirection.InOut, 1,
    PipeTransmissionMode.Byte, PipeOptions.Asynchronous);
using var clientStream = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);

var connectTask = serverStream.WaitForConnectionAsync();
await clientStream.ConnectAsync(2000);
await connectTask;

using var server = new RpcProtocol(new MessageStream(serverStream),
    heartbeatInterval: TimeSpan.Zero, requiredToken: "aot-token");
using var client = new RpcProtocol(new MessageStream(clientStream),
    heartbeatInterval: TimeSpan.Zero, authToken: "aot-token");

// 服务端：typed handler（返回值按源生成元数据序列化）
server.RegisterMethod<AddResult>("add", args =>
{
    // 位置参数：args[0] 是客户端用 JsonTypeInfo<AddArgs> 序列化出的对象
    var input = RpcProtocol.Convert<AddArgs>(args[0], ctx.AddArgs)!;
    return new AddResult(input.A + input.B);
}, ctx.AddResult);
// 服务端：直接返回 JsonElement（读侧不改写）
server.RegisterMethod("echo", args => args.Length > 0 ? args[0] : null);
// 服务端：二进制响应
server.RegisterMethod("blob", _ => new byte[] { 0, 1, 2, 0xFF });
// 服务端：二进制请求的 handler
server.RegisterMethod<UploadResult>("upload", args =>
{
    var data = (byte[])args[0]!;
    var tag = args.Length > 1 && args[1] is JsonElement t ? t.GetString() ?? "" : "";
    return new UploadResult(data.Length, tag);
}, ctx.UploadResult);
// 服务端主动推事件
server.RegisterMethod("push", async _ =>
{
    await server.SendJsonAsync(new AddResult(42), ctx.AddResult);
    return "pushed";
});

var eventSeen = new TaskCompletionSource<AddResult?>(TaskCreationOptions.RunContinuationsAsynchronously);
client.Event += e =>
{
    if (e.ContentType == ContentType.Json && e.Content is JsonElement el)
        eventSeen.TrySetResult(RpcProtocol.Convert(el, ctx.AddResult));
};

// 1) 自动 hello + 类型化请求
var sum = await client.SendRequestAsync("add",
    RpcProtocol.Args(RpcProtocol.Json(new AddArgs(3, 4), ctx.AddArgs)), ctx.AddResult);
Check("typed request (auto hello)", sum?.Sum == 7);

// 2) JsonElement 回显
var echo = await client.SendRequestAsync("echo", RpcProtocol.Json(new AddArgs(1, 2), ctx.AddArgs));
Check("JsonElement echo", echo is JsonElement je && je.GetProperty("A").GetInt32() == 1);

// 3) 文本/二进制响应
Check("binary response", ((byte[]?)await client.SendRequestAsync("blob"))!.SequenceEqual(new byte[] { 0, 1, 2, 0xFF }));

// 4) 二进制请求（服务端收到 [data, ...params]）
var upload = await client.SendRequestBinaryAsync("upload", new byte[] { 9, 8, 7 }, "tag-1");
Check("binary request", upload is JsonElement ur && ur.GetProperty("Size").GetInt32() == 3
    && ur.GetProperty("Tag").GetString() == "tag-1");

// 5) 类型化事件
await client.SendRequestAsync("push");
var evt = await Task.WhenAny(eventSeen.Task, Task.Delay(3000)) == eventSeen.Task ? eventSeen.Task.Result : null;
Check("typed event", evt?.Sum == 42);

// 6) 反射被禁用时，任意 POCO 必须快速失败并给出明确提示
var reflectionDisabled = !JsonSerializer.IsReflectionEnabledByDefault;
try
{
    await server.SendJsonAsync(new { arbitrary = 1 });
    Check("POCO rejected under AOT", false);
}
catch (Exception ex)
{
    Check($"POCO rejected under AOT ({ex.GetType().Name})", !reflectionDisabled || ex is NotSupportedException);
}

Console.WriteLine($"reflection-based serialization enabled: {JsonSerializer.IsReflectionEnabledByDefault}");
Console.WriteLine(failures == 0 ? "AOT SMOKE OK" : $"AOT SMOKE FAILED ({failures})");
return failures == 0 ? 0 : 1;
