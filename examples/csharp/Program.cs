// C# 使用示例（可与其他语言实现互操作）。
//   服务端: dotnet run --project examples/csharp -- server [port]
//   客户端: dotnet run --project examples/csharp -- client <port>
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using IpcProtocol;

if (args.Length == 0)
{
    Console.WriteLine("usage: Example server [port] | client <port>");
    return 1;
}

if (args[0] == "server")
{
    int port = args.Length > 1 ? int.Parse(args[1]) : 0;
    var listener = new TcpListener(IPAddress.Loopback, port);
    listener.Start();
    Console.WriteLine($"{{\"port\": {((IPEndPoint)listener.LocalEndpoint).Port}}}");
    while (true)
    {
        var socket = await listener.AcceptTcpClientAsync();
        var stream = new MessageStream(socket.GetStream());
        var rpc = new RpcProtocol(stream);
        rpc.Error += err => Console.Error.WriteLine($"[csharp_server] stream error: {err.Message}");

        rpc.RegisterMethod("add", a =>
            RpcProtocol.Convert<int>(a[0]) + RpcProtocol.Convert<int>(a[1]));
        rpc.RegisterMethod("echo", a => a[0]);
        rpc.RegisterMethod("greet", a => $"hello {RpcProtocol.Convert<string>(a[0])}");
        rpc.RegisterMethod("blob", _ => new byte[] { 0x00, 0x01, 0x02, 0xFF });
        rpc.RegisterMethod("nothing", _ => (object?)null);
        rpc.RegisterMethod("fail", _ => throw new Exception("boom"));
        rpc.RegisterMethod("upload", a =>
        {
            var data = (byte[])a[0]!;
            return new { size = data.Length, tag = RpcProtocol.Convert<string>(a[1]), head = data.Take(4).ToArray() };
        });
        rpc.RegisterMethod("pushEvents", async _ =>
        {
            await rpc.SendJsonAsync(new { push = 1 });
            await rpc.SendTextAsync("srv-text");
            await rpc.SendBinaryAsync(new byte[] { 1, 2 });
            return "sent";
        });
    }
}
else if (args[0] == "client")
{
    int port = int.Parse(args[1]);
    int passed = 0;
    void Check(string name, bool cond)
    {
        if (!cond) throw new Exception($"FAILED: {name}");
        passed++;
        Console.WriteLine($"  ok: {name}");
    }

    using var socket = new TcpClient();
    await socket.ConnectAsync(IPAddress.Loopback, port);
    using var client = new RpcProtocol(new MessageStream(socket.GetStream()));
    client.Error += err => Console.Error.WriteLine($"[csharp_client] stream error: {err.Message}");
    var events = new List<RpcEvent>();
    client.Event += e => { lock (events) events.Add(e); };

    Check("json add", RpcProtocol.Convert<int>(await client.SendRequestAsync("add", new object[] { 3, 4 })) == 7);
    var obj = new { name = "测试", list = new object[] { 1, 2.5, true, null! } };
    var echo = await client.SendRequestAsync<JsonElement>("echo", new object[] { obj });
    Check("json echo unicode", echo.GetProperty("name").GetString() == "测试" && echo.GetProperty("list")[1].GetDouble() == 2.5);
    Check("text response", (string?)await client.SendRequestAsync("greet", new object[] { "世界" }) == "hello 世界");
    Check("binary response", ((byte[]?)await client.SendRequestAsync("blob"))!.SequenceEqual(new byte[] { 0, 1, 2, 0xFF }));
    Check("null response", await client.SendRequestAsync("nothing") is JsonElement { ValueKind: JsonValueKind.Null });

    try { await client.SendRequestAsync("fail"); Check("error response", false); }
    catch (RpcException e) { Check("error response", (string?)e.Error == "boom"); }

    try { await client.SendRequestAsync("nope"); Check("method not found", false); }
    catch (RpcException e) { Check("method not found", e.Error!.ToString()!.Contains("Method not found: nope")); }

    var payload = new byte[256 * 300];
    for (int i = 0; i < payload.Length; i++) payload[i] = (byte)i;
    var up = (JsonElement)(await client.SendRequestBinaryAsync("upload", payload, "tag-1"))!;
    Check("binary request", up.GetProperty("size").GetInt32() == payload.Length
        && up.GetProperty("tag").GetString() == "tag-1"
        && up.GetProperty("head")[3].GetInt32() == 3);

    Check("pushEvents", (string?)await client.SendRequestAsync("pushEvents") == "sent");
    await Task.Delay(300);
    lock (events)
    {
        Check("events received", events.Count == 3
            && events[0].ContentType == ContentType.Json && ((JsonElement)events[0].Content!).GetProperty("push").GetInt32() == 1
            && events[1].ContentType == ContentType.Text && (string)events[1].Content! == "srv-text"
            && events[2].ContentType == ContentType.Binary && ((byte[])events[2].Content!).SequenceEqual(new byte[] { 1, 2 }));
    }

    var results = await Task.WhenAll(Enumerable.Range(0, 50)
        .Select(i => client.SendRequestAsync<int>("add", new object[] { i, i })));
    Check("concurrent requests", results.SequenceEqual(Enumerable.Range(0, 50).Select(i => i * 2)));

    Console.WriteLine($"\nALL {passed} CSHARP-CLIENT TESTS PASSED");
}
else
{
    Console.WriteLine("usage: Example server [port] | client <port>");
    return 1;
}
return 0;
