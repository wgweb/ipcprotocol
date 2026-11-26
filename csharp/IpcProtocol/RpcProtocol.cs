// RpcProtocol.cs — RpcProtocol.js 的 C# 移植版（线格式完全兼容）。
//
// 消息格式: [type u8][contentType u8][UUID 16B][payload...]
//   type:        0=REQUEST, 1=RESPONSE, 2=EVENT
//   contentType: 0=TEXT(utf8), 1=JSON(utf8), 2=BINARY(原始字节)
// JSON 请求体: {"method":...,"params":...,"to":...(可选)}；错误响应体: {"error":...}
// 二进制请求体: [methodLen u32BE][method utf8][paramsLen u32BE][params JSON utf8][binary...]
//   服务端 handler 收到的参数为 [binary, ...params]

using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace IpcProtocol;

public enum MessageType : byte { Request = 0, Response = 1, Event = 2, Ping = 3, Pong = 4, Hello = 5, HelloAck = 6 }
public enum ContentType : byte { Text = 0, Json = 1, Binary = 2 }

/// <summary>对端返回的错误响应，Error 属性携带原始 error 值（string 或 JsonElement）；Code 为错误码（如 ETIMEDOUT/EREMOTE）。</summary>
public sealed class RpcException : Exception
{
    public object? Error { get; }
    public string? Code { get; }
    public RpcException(object? error, string? code = null) : base(error?.ToString() ?? "null")
    {
        Error = error;
        Code = code;
    }
}

public sealed class RpcEvent
{
    public ContentType ContentType { get; init; }
    public object? Content { get; init; }
}

public sealed class RpcProtocol : IDisposable
{
    public static class Types
    {
        public const byte Request = 0, Response = 1, Event = 2, Ping = 3, Pong = 4, Hello = 5, HelloAck = 6;
    }
    public static class ContentTypes
    {
        public const byte Text = 0, Json = 1, Binary = 2;
    }

    /// <summary>收到事件消息（sendText/sendJson/sendBinary）。</summary>
    public event Action<RpcEvent>? Event;
    /// <summary>底层流错误。</summary>
    public event Action<Exception>? Error;

    private MessageStream? _messageStream;
    private readonly ConcurrentDictionary<string, Func<object?[], Task<object?>>> _handlers = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<object?>> _pendingRequests = new();
    private volatile bool _disposed;

    /// <summary>默认 RPC 超时，对齐 JS 修复版 15s；TimeSpan.Zero 表示关闭。</summary>
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromSeconds(15);

    // v1.1：心跳与可选 HELLO 握手（PROTOCOL.md §8.1/§8.2）
    public TimeSpan HeartbeatInterval { get; }
    public TimeSpan HeartbeatTimeout { get; }
    public string? AuthToken { get; }
    public string? RequiredToken { get; }
    private bool _authenticated;
    private int _unauthorizedHits;
    private long _lastReceivedTicks = Environment.TickCount64;
    private readonly CancellationTokenSource? _heartbeatCts;
    private const int ProtocolVersion = 1;

    // v1.1.2：能力协商（PROTOCOL.md §8.4）
    public static readonly string[] DefaultCapabilities = { "ping", "binary", "event" };
    public string[] Capabilities { get; }
    public string ImplName { get; set; } = "csharp";
    /// <summary>对端在 HELLO 中声明的能力；null 表示未声明（旧实现）。</summary>
    public string[]? PeerCapabilities { get; private set; }
    /// <summary>客户端收到 HELLO_ACK 时服务端声明的能力。</summary>
    public string[]? ServerCapabilities { get; private set; }
    /// <summary>因能力协商停用心跳的原因；null 表示心跳正常。</summary>
    public string? HeartbeatDisabledReason { get; private set; }
    private readonly SemaphoreSlim _authLock = new(1, 1);

    public RpcProtocol(MessageStream messageStream, TimeSpan? heartbeatInterval = null,
        TimeSpan? heartbeatTimeout = null, string? authToken = null, string? requiredToken = null,
        string[]? capabilities = null)
    {
        _messageStream = messageStream ?? throw new ArgumentNullException(nameof(messageStream));
        HeartbeatInterval = heartbeatInterval ?? TimeSpan.FromSeconds(5);
        HeartbeatTimeout = heartbeatTimeout ?? TimeSpan.FromSeconds(15);
        // v1.1.2：未显式传 token 时从环境变量继承（Node 主进程注入 IPC_TOKEN）
        AuthToken = authToken ?? Environment.GetEnvironmentVariable("IPC_TOKEN");
        RequiredToken = requiredToken;
        Capabilities = capabilities ?? DefaultCapabilities;
        _messageStream.Message += OnMessage;
        _messageStream.Error += OnStreamError;
        // 先挂好监听器再启动读循环，避免早到消息在注册前被分发的竞态
        _messageStream.Start();
        if (HeartbeatInterval > TimeSpan.Zero)
        {
            _heartbeatCts = new CancellationTokenSource();
            _ = HeartbeatLoopAsync(_heartbeatCts.Token);
        }
    }

    // ========== 心跳（§8.1） ==========

    private async Task HeartbeatLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested && !_disposed)
        {
            try { await Task.Delay(HeartbeatInterval, ct).ConfigureAwait(false); }
            catch (OperationCanceledException) { return; }
            if (_disposed || HeartbeatDisabledReason != null) return;
            long silent = Environment.TickCount64 - Interlocked.Read(ref _lastReceivedTicks);
            if (HeartbeatTimeout > TimeSpan.Zero && silent >= (long)HeartbeatTimeout.TotalMilliseconds)
            {
                var err = new RpcException($"heartbeat timeout: no data for {silent}ms", "EHEARTBEAT");
                foreach (var key in _pendingRequests.Keys)
                    if (_pendingRequests.TryRemove(key, out var pending))
                        pending.TrySetException(err);
                Error?.Invoke(err);
                Dispose();
                return;
            }
            if (silent >= (long)HeartbeatInterval.TotalMilliseconds)
            {
                try { await SendRawAsync(Types.Ping, ContentTypes.Text, Guid.NewGuid().ToString(), "").ConfigureAwait(false); }
                catch { /* ignore */ }
            }
        }
    }

    // ========== HELLO 握手（§8.2） ==========

    private async Task HandleHelloAsync(string id, object? content)
    {
        int? version = content is JsonElement j && j.ValueKind == JsonValueKind.Object
            && j.TryGetProperty("version", out var v) && v.ValueKind == JsonValueKind.Number
            ? v.GetInt32() : null;
        string? token = content is JsonElement jt && jt.ValueKind == JsonValueKind.Object
            && jt.TryGetProperty("token", out var t) && t.ValueKind == JsonValueKind.String
            ? t.GetString() : null;
        byte[] ack;
        bool fail = false;
        if (version != ProtocolVersion)
        {
            ack = JsonPayload.Error($"unsupported protocol version: {version}"); fail = true;
        }
        else if (!string.IsNullOrEmpty(RequiredToken) && token != RequiredToken)
        {
            ack = JsonPayload.Error("unauthorized"); fail = true;
        }
        else
        {
            _authenticated = true;
            PeerCapabilities = ReadCaps(content);
            var peerCaps = PeerCapabilities;
            if (peerCaps != null && !peerCaps.Contains("ping"))
                DisableHeartbeat($"peer {ReadString(content, "impl") ?? "unknown"} declares no 'ping' capability");
            ack = JsonPayload.HelloAck(true, ProtocolVersion, Capabilities, ImplName);
        }
        try { await SendRawBytesAsync(Types.HelloAck, ContentTypes.Json, id, ack).ConfigureAwait(false); }
        catch { /* ignore */ }
        if (fail) Dispose();
    }

    private void HandleHelloAck(string id, object? content)
    {
        if (!_pendingRequests.TryRemove(id, out var pending)) return;
        if (content is JsonElement json && json.ValueKind == JsonValueKind.Object)
        {
            if (json.TryGetProperty("error", out var error) && IsTruthy(error))
            {
                pending.TrySetException(new RpcException(
                    error.ValueKind == JsonValueKind.String ? error.GetString() : (object?)error, "EAUTH"));
                return;
            }
            if (json.TryGetProperty("version", out var v) && v.ValueKind == JsonValueKind.Number
                && v.GetInt32() == ProtocolVersion)
            {
                ServerCapabilities = ReadCaps(content);
                if (ServerCapabilities != null && !ServerCapabilities.Contains("ping"))
                    DisableHeartbeat($"server {ReadString(content, "impl") ?? "unknown"} declares no 'ping' capability");
                _authenticated = true;
                pending.TrySetResult(content);
                return;
            }
            pending.TrySetException(new RpcException($"unsupported protocol version: {json}", "EVERSION"));
        }
        else
        {
            pending.TrySetException(new RpcException("unsupported protocol version: null", "EVERSION"));
        }
    }

    /// <summary>显式握手：启用鉴权时调用方必须先 await HelloAsync() 再发业务请求。</summary>
    public async Task<object?> HelloAsync(TimeSpan? timeout = null)
    {
        var id = Guid.NewGuid().ToString();
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[id] = tcs;
        try
        {
            await SendRawBytesAsync(Types.Hello, ContentTypes.Json, id,
                JsonPayload.Hello(ProtocolVersion, AuthToken, Capabilities, ImplName)).ConfigureAwait(false);
        }
        catch (Exception err)
        {
            _pendingRequests.TryRemove(id, out _);
            tcs.TrySetException(err);
        }
        return await WaitPendingAsync(id, tcs, "hello", timeout).ConfigureAwait(false);
    }

    // ========== 序列化 ==========

    public static byte[] Serialize(byte type, byte contentType, string id, object? data)
        => SerializeRaw(type, contentType, id, SerializePayload(contentType, data));

    /// <summary>用已编码好的载荷组帧（协议内部报文走这条路，完全不经过序列化器）。</summary>
    public static byte[] SerializeRaw(byte type, byte contentType, string id, byte[] payload)
    {
        var header = new byte[18];
        header[0] = type;
        header[1] = contentType;
        System.Convert.FromHexString(id.Replace("-", "")).CopyTo(header, 2);
        var result = new byte[18 + payload.Length];
        header.CopyTo(result, 0);
        payload.CopyTo(result, 18);
        return result;
    }

    public static (byte Type, byte ContentType, string Id, object? Content) Deserialize(byte[] buffer)
    {
        if (buffer.Length < 18)
            throw new InvalidDataException("Failed to deserialize message: buffer too short");
        byte type = buffer[0], contentType = buffer[1];
        string hex = System.Convert.ToHexString(buffer, 2, 16).ToLowerInvariant();
        string id = $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..]}";
        var payload = buffer.AsSpan(18);
        object? content = contentType switch
        {
            ContentTypes.Text => Encoding.UTF8.GetString(payload),
            ContentTypes.Json => JsonDocument.Parse(payload.ToArray()).RootElement.Clone(),
            ContentTypes.Binary => payload.ToArray(),
            _ => throw new InvalidDataException($"Failed to deserialize message: unknown content type {contentType}"),
        };
        return (type, contentType, id, content);
    }

    public static byte[] EncodeBinaryRequest(string method, object?[] parameters, byte[] binaryData)
    {
        var methodBuf = Encoding.UTF8.GetBytes(method);
        var paramsBuf = JsonPayload.Serialize(parameters);
        var result = new byte[4 + methodBuf.Length + 4 + paramsBuf.Length + binaryData.Length];
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(offset), (uint)methodBuf.Length); offset += 4;
        methodBuf.CopyTo(result, offset); offset += methodBuf.Length;
        BinaryPrimitives.WriteUInt32BigEndian(result.AsSpan(offset), (uint)paramsBuf.Length); offset += 4;
        paramsBuf.CopyTo(result, offset); offset += paramsBuf.Length;
        binaryData.CopyTo(result, offset);
        return result;
    }

    public static (string Method, JsonElement Params, byte[] BinaryData) DecodeBinaryRequest(byte[] buffer)
    {
        int offset = 0;
        if (buffer.Length < 4)
            throw new InvalidDataException("binary request too short");
        uint methodLength = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset)); offset += 4;
        if (buffer.Length < offset + (long)methodLength + 4)
            throw new InvalidDataException("binary request truncated (method)");
        string method = Encoding.UTF8.GetString(buffer, offset, (int)methodLength); offset += (int)methodLength;
        uint paramsLength = BinaryPrimitives.ReadUInt32BigEndian(buffer.AsSpan(offset)); offset += 4;
        if (buffer.Length < offset + (long)paramsLength)
            throw new InvalidDataException("binary request truncated (params)");
        var parameters = JsonDocument.Parse(buffer.AsMemory(offset, (int)paramsLength)).RootElement.Clone(); offset += (int)paramsLength;
        return (method, parameters, buffer.AsSpan(offset).ToArray());
    }

    // ========== 接收处理 ==========

    private void OnMessage(byte[] buffer)
    {
        Interlocked.Exchange(ref _lastReceivedTicks, Environment.TickCount64);  // 任何入站消息刷新心跳活动
        try
        {
            var (type, contentType, id, content) = Deserialize(buffer);
            if (type > 6) throw new InvalidDataException($"Invalid message type: {type}");
            if (contentType > 2) throw new InvalidDataException($"Invalid content type: {contentType}");

            switch (type)
            {
                case Types.Request:
                    if (!string.IsNullOrEmpty(RequiredToken) && !_authenticated)
                    {
                        _ = RejectUnauthorizedAsync(id);
                        break;
                    }
                    _ = HandleRequestAsync(id, content);
                    break;
                case Types.Response:
                    HandleResponse(id, content);
                    break;
                case Types.Event:
                    if (!string.IsNullOrEmpty(RequiredToken) && !_authenticated) break;  // 未鉴权事件丢弃
                    Event?.Invoke(new RpcEvent { ContentType = (ContentType)contentType, Content = content });
                    break;
                case Types.Ping:
                    _ = SendPongAsync(id);
                    break;
                case Types.Pong:
                    break;  // 仅刷新 _lastReceivedTicks，不交付应用层
                case Types.Hello:
                    _ = HandleHelloAsync(id, content);
                    break;
                case Types.HelloAck:
                    HandleHelloAck(id, content);
                    break;
            }
        }
        catch (Exception err)
        {
            Console.Error.WriteLine($"Failed to handle message: {err.Message}");
            // 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版）
            if (buffer.Length >= 18 && buffer[0] == Types.Request)
            {
                try
                {
                    string hex = System.Convert.ToHexString(buffer, 2, 16).ToLowerInvariant();
                    string id = $"{hex[..8]}-{hex[8..12]}-{hex[12..16]}-{hex[16..20]}-{hex[20..]}";
                    _ = SendResponseBytesAsync(id, JsonPayload.Error("Invalid request payload"));
                }
                catch { /* ignore */ }
            }
        }
    }

    private async Task SendPongAsync(string id)
    {
        try { await SendRawAsync(Types.Pong, ContentTypes.Text, id, "").ConfigureAwait(false); }
        catch { /* ignore */ }
    }

    private async Task RejectUnauthorizedAsync(string id)
    {
        // 未鉴权：回错误但不断链（允许随后 hello 重试）；连续 5 次才强制断开
        int hits = Interlocked.Increment(ref _unauthorizedHits);
        try { await SendResponseBytesAsync(id, JsonPayload.Error("Unauthorized")).ConfigureAwait(false); }
        catch { /* ignore */ }
        if (hits >= 5) Dispose();
    }

    private async Task HandleRequestAsync(string id, object? request)
    {
        string? method;
        object?[] args;

        try
        {
            if (request is byte[] binary)
            {
                var (m, parameters, binaryData) = DecodeBinaryRequest(binary);
                method = m;
                var list = new List<object?> { binaryData };
                if (parameters.ValueKind == JsonValueKind.Array)
                    foreach (var item in parameters.EnumerateArray()) list.Add(item);
                args = list.ToArray();
            }
            else if (request is JsonElement json && json.ValueKind == JsonValueKind.Object)
            {
                method = json.TryGetProperty("method", out var m) && m.ValueKind == JsonValueKind.String ? m.GetString() : null;
                if (json.TryGetProperty("params", out var p) && p.ValueKind == JsonValueKind.Array)
                    args = p.EnumerateArray().Cast<object?>().ToArray();
                else if (json.TryGetProperty("params", out p) && p.ValueKind != JsonValueKind.Null && p.ValueKind != JsonValueKind.Undefined)
                    args = new object?[] { p };
                else
                    args = Array.Empty<object?>();
            }
            else
            {
                throw new InvalidDataException("request payload is not an object");
            }
            if (string.IsNullOrEmpty(method))
                throw new InvalidDataException("request method missing");
        }
        catch (Exception err)
        {
            await SendResponseBytesAsync(id, JsonPayload.Error($"Invalid request payload: {err.Message}")).ConfigureAwait(false);
            return;
        }

        if (!_handlers.TryGetValue(method, out var handler))
        {
            await SendResponseBytesAsync(id, JsonPayload.Error($"Method not found: {method}")).ConfigureAwait(false);
            return;
        }

        try
        {
            var result = await handler(args).ConfigureAwait(false);
            byte contentType = ContentTypes.Json;
            if (result is string) contentType = ContentTypes.Text;
            else if (result is byte[]) contentType = ContentTypes.Binary;
            await SendResponseAsync(id, result, contentType).ConfigureAwait(false);
        }
        catch (Exception err)
        {
            await SendResponseBytesAsync(id, JsonPayload.Error(err.Message)).ConfigureAwait(false);
        }
    }

    private void HandleResponse(string id, object? response)
    {
        if (!_pendingRequests.TryRemove(id, out var pending)) return;

        if (response is JsonElement json && json.ValueKind == JsonValueKind.Object
            && json.TryGetProperty("error", out var error) && IsTruthy(error))
        {
            object? errorValue = error.ValueKind == JsonValueKind.String ? error.GetString() : (object?)error;
            pending.TrySetException(new RpcException(errorValue, "EREMOTE"));
        }
        else
        {
            pending.TrySetResult(response);
        }
    }

    // JS 真值语义：null/undefined/false/0/"" 为假
    private static bool IsTruthy(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined or JsonValueKind.False => false,
        JsonValueKind.String => !string.IsNullOrEmpty(e.GetString()),
        JsonValueKind.Number => e.GetDouble() != 0,
        _ => true,
    };

    // v1.1.2 能力协商辅助
    private static string[]? ReadCaps(object? content)
    {
        if (content is not JsonElement json || json.ValueKind != JsonValueKind.Object) return null;
        if (!json.TryGetProperty("caps", out var caps) || caps.ValueKind != JsonValueKind.Array) return null;
        var list = new List<string>();
        foreach (var item in caps.EnumerateArray())
            if (item.ValueKind == JsonValueKind.String) list.Add(item.GetString()!);
        return list.ToArray();
    }

    private static string? ReadString(object? content, string name)
    {
        if (content is not JsonElement json || json.ValueKind != JsonValueKind.Object) return null;
        return json.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
    }

    private void DisableHeartbeat(string reason)
    {
        if (HeartbeatDisabledReason != null) return;
        HeartbeatDisabledReason = reason;
        try { _heartbeatCts?.Cancel(); } catch { /* ignore */ }
    }

    private void OnStreamError(Exception error)
    {
        string? code = (error as StreamException)?.Code;  // 错误码透传（ERESYNC/ERESYNC_TIMEOUT 等）
        foreach (var key in _pendingRequests.Keys)
        {
            if (_pendingRequests.TryRemove(key, out var pending))
                pending.TrySetException(new RpcException($"Request failed: {error.Message}", code));
        }
        Error?.Invoke(error);
    }

    // ========== 发送 ==========

    public Task SendRawAsync(byte type, byte contentType, string id, object? data)
    {
        return SendRawBytesAsync(type, contentType, id, SerializePayload(contentType, data));
    }

    /// <summary>直接发送已编码载荷（协议内部报文与 AOT 场景用这条路径）。</summary>
    public Task SendRawBytesAsync(byte type, byte contentType, string id, byte[] payload)
    {
        var stream = _messageStream ?? throw new InvalidOperationException("messageStream is not initialized");
        return stream.SendAsync(SerializeRaw(type, contentType, id, payload));
    }

    private static byte[] SerializePayload(byte contentType, object? data) => contentType switch
    {
        ContentTypes.Text => Encoding.UTF8.GetBytes((string)data!),
        ContentTypes.Json => JsonPayload.Serialize(data),
        ContentTypes.Binary => (byte[])data!,
        _ => throw new NotSupportedException($"Unsupported content type: {contentType}"),
    };

    /// <summary>等待响应；超时移除 pending 并抛 RpcException(Code="ETIMEDOUT")，对齐 JS 修复版。</summary>
    private async Task<object?> WaitPendingAsync(string id, TaskCompletionSource<object?> tcs, string method, TimeSpan? timeout)
    {
        var effective = timeout ?? DefaultTimeout;
        try
        {
            return effective > TimeSpan.Zero
                ? await tcs.Task.WaitAsync(effective).ConfigureAwait(false)
                : await tcs.Task.ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _pendingRequests.TryRemove(id, out _);
            throw new RpcException($"RPC timeout after {(long)effective.TotalMilliseconds}ms: {method}", "ETIMEDOUT");
        }
    }

    /// <summary>首次发业务请求前自动握手（取环境变量或构造参数里的 token），并发安全只握手一次。</summary>
    public async Task EnsureAuthAsync()
    {
        if (string.IsNullOrEmpty(AuthToken) || _authenticated) return;
        await _authLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_authenticated) return;
            await HelloAsync().ConfigureAwait(false);
            _authenticated = true;
        }
        finally { _authLock.Release(); }
    }

    /// <summary>发送 JSON 请求并等待响应。返回 string(TEXT)/byte[](BINARY)/JsonElement(JSON)。
    /// timeout 为 null 用 DefaultTimeout，TimeSpan.Zero 关闭超时。</summary>
    public async Task<object?> SendRequestAsync(string method, object? parameters = null, string? to = null, TimeSpan? timeout = null)
    {
        await EnsureAuthAsync().ConfigureAwait(false);
        var id = Guid.NewGuid().ToString();
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[id] = tcs;
        try
        {
            await SendRawBytesAsync(Types.Request, ContentTypes.Json, id,
                JsonPayload.Request(method, parameters, to)).ConfigureAwait(false);
        }
        catch (Exception err)
        {
            _pendingRequests.TryRemove(id, out _);
            tcs.TrySetException(err);
        }
        return await WaitPendingAsync(id, tcs, method, timeout).ConfigureAwait(false);
    }

    /// <summary>发送 JSON 请求并把 JSON 响应反序列化为 T（反射版，Native AOT 下不可用）。</summary>
    [RequiresUnreferencedCode("反射反序列化需要运行时元数据；Native AOT 请用带 JsonTypeInfo<T> 的重载。")]
    [RequiresDynamicCode("反射反序列化在 Native AOT 下不可用；请用带 JsonTypeInfo<T> 的重载。")]
    public async Task<T?> SendRequestAsync<T>(string method, object? parameters = null, string? to = null, TimeSpan? timeout = null)
    {
        var result = await SendRequestAsync(method, parameters, to, timeout).ConfigureAwait(false);
        return Convert<T>(result);
    }

    /// <summary>发送 JSON 请求并按 JsonTypeInfo&lt;T&gt; 反序列化响应（Native AOT 安全）。</summary>
    public async Task<T?> SendRequestAsync<T>(string method, object? parameters, JsonTypeInfo<T> responseType,
        string? to = null, TimeSpan? timeout = null)
    {
        var result = await SendRequestAsync(method, parameters, to, timeout).ConfigureAwait(false);
        return Convert(result, responseType);
    }

    /// <summary>发送二进制请求并等待响应（默认超时）。服务端 handler 收到 [data, ...args]。</summary>
    public Task<object?> SendRequestBinaryAsync(string method, byte[] data, params object?[] args)
        => SendRequestBinaryWithTimeoutAsync(DefaultTimeout, method, data, args);

    /// <summary>发送二进制请求并等待响应；TimeSpan.Zero 关闭超时。</summary>
    public async Task<object?> SendRequestBinaryWithTimeoutAsync(TimeSpan timeout, string method, byte[] data, params object?[] args)
    {
        await EnsureAuthAsync().ConfigureAwait(false);
        var id = Guid.NewGuid().ToString();
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[id] = tcs;
        var binaryRequest = EncodeBinaryRequest(method, args, data);
        try
        {
            await SendRawAsync(Types.Request, ContentTypes.Binary, id, binaryRequest).ConfigureAwait(false);
        }
        catch (Exception err)
        {
            _pendingRequests.TryRemove(id, out _);
            tcs.TrySetException(err);
        }
        return await WaitPendingAsync(id, tcs, method, timeout).ConfigureAwait(false);
    }

    public Task SendResponseAsync(string id, object? response, byte contentType = ContentTypes.Json)
        => SendRawAsync(Types.Response, contentType, id, response);

    /// <summary>发送已编码的 JSON 响应（AOT 场景或自建载荷时使用）。</summary>
    public Task SendResponseBytesAsync(string id, byte[] jsonPayload)
        => SendRawBytesAsync(Types.Response, ContentTypes.Json, id, jsonPayload);

    // ========== 辅助方法 ==========

    public Task SendTextAsync(string text)
        => SendRawAsync(Types.Event, ContentTypes.Text, Guid.NewGuid().ToString(), text);

    public Task SendBinaryAsync(byte[] data)
        => SendRawAsync(Types.Event, ContentTypes.Binary, Guid.NewGuid().ToString(), data);

    public Task SendJsonAsync(object? data)
        => SendRawAsync(Types.Event, ContentTypes.Json, Guid.NewGuid().ToString(), data);

    /// <summary>发送 JSON 事件（JsonTypeInfo 版本，Native AOT 安全）。</summary>
    public Task SendJsonAsync<T>(T data, JsonTypeInfo<T> typeInfo)
        => SendRawBytesAsync(Types.Event, ContentTypes.Json, Guid.NewGuid().ToString(),
            JsonSerializer.SerializeToUtf8Bytes(data, typeInfo));

    /// <summary>注册异步方法处理器。JSON 参数以 JsonElement 传入，可用 Convert&lt;T&gt; 转换。</summary>
    public void RegisterMethod(string name, Func<object?[], Task<object?>> handler)
        => _handlers[name] = handler;

    /// <summary>注册同步方法处理器。</summary>
    public void RegisterMethod(string name, Func<object?[], object?> handler)
        => _handlers[name] = args => Task.FromResult(handler(args));

    /// <summary>注册异步处理器，返回值按 JsonTypeInfo&lt;T&gt; 序列化（Native AOT 安全）。</summary>
    public void RegisterMethod<T>(string name, Func<object?[], Task<T>> handler, JsonTypeInfo<T> resultType)
        => _handlers[name] = async args =>
            JsonSerializer.SerializeToElement(await handler(args).ConfigureAwait(false), resultType);

    /// <summary>注册同步处理器，返回值按 JsonTypeInfo&lt;T&gt; 序列化（Native AOT 安全）。</summary>
    public void RegisterMethod<T>(string name, Func<object?[], T> handler, JsonTypeInfo<T> resultType)
        => _handlers[name] = args =>
            Task.FromResult<object?>(JsonSerializer.SerializeToElement(handler(args), resultType));

    /// <summary>把强类型值转成 JsonElement（调用 JsonTypeInfo，Native AOT 安全）。</summary>
    public static JsonElement Json<T>(T value, JsonTypeInfo<T> typeInfo)
        => JsonSerializer.SerializeToElement(value, typeInfo);

    /// <summary>把若干 JsonElement 组装成位置参数数组（可直接作为 parameters 传入）。</summary>
    public static JsonElement Args(params JsonElement[] args)
    {
        var bytes = JsonPayload.Build(w =>
        {
            w.WriteStartArray();
            foreach (var arg in args) arg.WriteTo(w);
            w.WriteEndArray();
        });
        using var doc = JsonDocument.Parse(bytes);
        return doc.RootElement.Clone();
    }

    /// <summary>把 JSON 参数/结果（JsonElement）转换为强类型对象（反射版，Native AOT 下不可用）。</summary>
    [RequiresUnreferencedCode("反射反序列化需要运行时元数据；Native AOT 请用 Convert<T>(value, JsonTypeInfo<T>)。")]
    [RequiresDynamicCode("反射反序列化在 Native AOT 下不可用；请用 Convert<T>(value, JsonTypeInfo<T>)。")]
    public static T? Convert<T>(object? value)
    {
        if (value is T typed) return typed;
        if (value is null) return default;
        if (value is JsonElement element) return JsonSerializer.Deserialize<T>(element);
        return (T)System.Convert.ChangeType(value, typeof(T))!;
    }

    /// <summary>把 JSON 参数/结果转换为强类型对象（JsonTypeInfo 版本，Native AOT 安全）。</summary>
    public static T? Convert<T>(object? value, JsonTypeInfo<T> typeInfo)
    {
        if (value is T typed) return typed;
        if (value is null) return default;
        if (value is JsonElement element) return JsonSerializer.Deserialize(element, typeInfo);
        return (T)System.Convert.ChangeType(value, typeof(T))!;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _heartbeatCts?.Cancel();
        foreach (var key in _pendingRequests.Keys)
        {
            if (_pendingRequests.TryRemove(key, out var pending))
                pending.TrySetException(new RpcException("RPC Protocol destroyed"));
        }
        if (_messageStream is not null)
        {
            _messageStream.Message -= OnMessage;
            _messageStream.Error -= OnStreamError;
            _messageStream.Dispose();
            _messageStream = null;
        }
        _handlers.Clear();
        Event = null;
        Error = null;
        Console.WriteLine("[RpcProtocol] Destroyed");
    }
}