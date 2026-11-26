// C# 子进程 <-> Node 主进程，Windows 命名管道互操作测试
using System.IO.Pipes;
using IpcProtocol;

class Program
{
    static int passed = 0;
    static readonly List<(ContentType ct, object? content)> Events = new();

    static bool IsNull(object? v) => v == null || (v is System.Text.Json.JsonElement je && je.ValueKind == System.Text.Json.JsonValueKind.Null);

    static void Check(string name, bool cond)
    {
        if (!cond) throw new Exception($"FAILED: {name}");
        passed++;
        Console.WriteLine($"  ok: {name}");
    }

    static async Task Main(string[] args)
    {
        var pipeName = args.Length > 0 ? args[0] : "ipc_interop_test";
        using var pipe = new NamedPipeClientStream(".", pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
        pipe.Connect(5000);
        Check("connect pipe", pipe.IsConnected);

        using var ms = new MessageStream(pipe);
        using var rpc = new RpcProtocol(ms, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(15));
        rpc.Error += e => Console.WriteLine($"[rpc error] {e.Message}");
        rpc.Event += ev => Events.Add((ev.ContentType, ev.Content));

        Check("json add", RpcProtocol.Convert<int>(await rpc.SendRequestAsync("add", new object?[] { 3, 4 })) == 7);
        Check("text response", (string?)await rpc.SendRequestAsync("text") == "hello-text-✓");

        var json = await rpc.SendRequestAsync("json", new object?[] { new Dictionary<string, object?> { ["name"] = "中文", ["arr"] = new object?[] { 1, 2, 3 } } });
        var asText = json?.ToString() ?? "";
        Check("unicode json roundtrip", asText.Contains("中文") || asText.Contains("\u4f60") || asText.Contains("cn"));

        Check("explicit null", IsNull(await rpc.SendRequestAsync("nul")));

        try { await rpc.SendRequestAsync("boom"); Check("error response", false); }
        catch (RpcException e) { Check("error response", e.Message.Contains("boom")); }

        try { await rpc.SendRequestAsync("no_such_method"); Check("method not found", false); }
        catch (RpcException e) { Check("method not found", e.Message.ToLower().Contains("not found")); }

        var payload = new byte[76800];
        for (int i = 0; i < payload.Length; i++) payload[i] = (byte)(i % 256);
        var echoed = await rpc.SendRequestBinaryAsync("binEcho", payload) as byte[];
        Check("binary 75KB echo", echoed != null && echoed.Length == payload.Length && echoed.SequenceEqual(payload));

        var info = await rpc.SendRequestBinaryAsync("len", new byte[] { 1, 2, 3, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 });
        Check("binary -> json result", (info?.ToString() ?? "").Contains("16"));

        Check("slow task 800ms", (string?)await rpc.SendRequestAsync("slow", new object?[] { 800 }, null, TimeSpan.FromSeconds(5)) == "slept-800");

        var tasks = Enumerable.Range(0, 50).Select(i => rpc.SendRequestAsync("add", new object?[] { i, i })).ToArray();
        var results = await Task.WhenAll(tasks);
        Check("50 concurrent", results.Select(v => RpcProtocol.Convert<int>(v)).SequenceEqual(Enumerable.Range(0, 50).Select(i => i * 2)));

        await rpc.SendRequestAsync("push", new object?[] { 3 });
        await Task.Delay(300);
        Check($"events received ({Events.Count})", Events.Count == 9);

        // 空闲 12s：Node 端心跳 5s/15s，C# 必须回 PONG 才能存活
        await Task.Delay(12000);
        Check("idle 12s survives heartbeat", RpcProtocol.Convert<int>(await rpc.SendRequestAsync("add", new object?[] { 1, 1 })) == 2);

        Console.WriteLine($"\nALL {passed} CSHARP-PIPE TESTS PASSED");
        pipe.Dispose();
    }
}


