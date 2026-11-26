// v1.1.2 回归：C# 客户端默认鉴权 + 能力协商（连 Node 命名管道服务端）
// 环境变量: PIPE_NAME、IPC_TOKEN（正确 token）、MODE=good|bad
using System.IO.Pipes;
using IpcProtocol;

int pass = 0, fail = 0;
void Ok(bool cond, string name, string extra = "")
{
    if (cond) { pass++; Console.WriteLine($"  PASS {name}"); }
    else { fail++; Console.WriteLine($"  FAIL {name} {extra}"); }
}

var pipe = Environment.GetEnvironmentVariable("PIPE_NAME")!;
var mode = Environment.GetEnvironmentVariable("MODE") ?? "good";
var token = Environment.GetEnvironmentVariable("IPC_TOKEN");

Console.WriteLine($"\n[C# 鉴权/{mode}] pipe={pipe} token={token}");

using var pipeStream = new NamedPipeClientStream(".", pipe, PipeDirection.InOut, PipeOptions.Asynchronous);
await pipeStream.ConnectAsync(5000);

// 注意：未显式传 authToken → 构造函数自动从 IPC_TOKEN 读取；SendRequestAsync 会自动 hello
using var proto = new RpcProtocol(new MessageStream(pipeStream), TimeSpan.Zero, TimeSpan.Zero);

try
{
    if (mode == "good")
    {
        var sum = RpcProtocol.Convert<int>(await proto.SendRequestAsync("add", new object?[] { 10, 32 }));
        Ok(sum == 42, "自动 hello 后业务调用", sum.ToString());
        Ok(proto.ServerCapabilities != null && proto.ServerCapabilities!.Contains("ping"),
            "拿到服务端能力", proto.ServerCapabilities == null ? "null" : string.Join(",", proto.ServerCapabilities));
        var peerCaps = await proto.SendRequestAsync("peerCaps");
        var names = new List<string?>();
        if (peerCaps is System.Text.Json.JsonElement je && je.ValueKind == System.Text.Json.JsonValueKind.Object
            && je.TryGetProperty("peer", out var arr) && arr.ValueKind == System.Text.Json.JsonValueKind.Array)
        {
            foreach (var item in arr.EnumerateArray())
                if (item.ValueKind == System.Text.Json.JsonValueKind.String) names.Add(item.GetString());
        }
        Ok(names.Contains("ping"), "服务端看到 C# 客户端能力", string.Join(",", names));
    }
    else
    {
        try
        {
            await proto.SendRequestAsync("add", new object?[] { 1, 2 });
            Ok(false, "错误 token 应被拒");
        }
        catch (RpcException err)
        {
            Ok(err.Code == "EAUTH", "错误 token → EAUTH", $"code={err.Code} msg={err.Message}");
        }
    }
}
catch (Exception err)
{
    fail++;
    Console.WriteLine($"  FAIL 异常: {err.GetType().Name} {err.Message}");
}

Console.WriteLine($"\n结果: {pass} passed, {fail} failed");
Environment.Exit(fail == 0 ? 0 : 1);
