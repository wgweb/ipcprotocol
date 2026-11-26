// MessageStream.cs — MessageStream.js 的 C# 移植版（线格式完全兼容）。
//
// 帧格式（正常模式，enableCRC=true 时）:
//   [长度 u32LE][长度头CRC32 u32LE][数据...][数据CRC32 u32LE]
// 关闭 CRC 时: [长度 u32LE][数据...]
//
// 错误包（按普通帧封装，5 字节）: [0xEEEEEEEE][错误类型 1B]
//   1=LENGTH_CRC_FAILED, 2=MESSAGE_CRC_FAILED, 3=BUFFER_OVERFLOW
// 重同步协议消息（裸帧，无 CRC）: [0xFFFFFFFF][长度 u32LE][数据(如 'RESET_CONFIRM')]
//
// CRC32 与 JS 版一致: 反射多项式 0xEDB88320，初值/终值 0xFFFFFFFF。

using System.Buffers.Binary;

namespace IpcProtocol;

/// <summary>带错误码的流错误（对齐 JS 修复版 ipcError 的 code 字段）。</summary>
public sealed class StreamException : Exception
{
    public string? Code { get; }
    public StreamException(string message, string? code = null) : base(message) { Code = code; }
}

public sealed class MessageStream : IDisposable
{
    private static readonly byte[] ProtocolHeader = { 0xFF, 0xFF, 0xFF, 0xFF };
    private static readonly byte[] ErrorHeader = { 0xEE, 0xEE, 0xEE, 0xEE };

    private static readonly Dictionary<string, byte> ErrorTypes = new()
    {
        ["LENGTH_CRC_FAILED"] = 1,
        ["MESSAGE_CRC_FAILED"] = 2,
        ["BUFFER_OVERFLOW"] = 3,
    };
    private static readonly Dictionary<byte, string> ErrorTypeCodes = new()
    {
        [1] = "LENGTH_CRC_FAILED",
        [2] = "MESSAGE_CRC_FAILED",
        [3] = "BUFFER_OVERFLOW",
    };

    /// <summary>收到一条完整消息（不含帧头/CRC）。</summary>
    public event Action<byte[]>? Message;
    /// <summary>流错误（CRC 校验失败、连接断开等）。</summary>
    public event Action<Exception>? Error;
    /// <summary>重同步完成（收到 RESET_CONFIRM）。</summary>
    public event Action? SyncComplete;

    private readonly Stream _stream;
    private readonly bool _enableCRC;
    private readonly long _maxBufferSize;
    private readonly int _syncTimeoutMs;
    // 接收缓冲：byte[] + 头游标，消费只前移游标，避免 O(n^2) 头部删除
    private byte[] _buffer = Array.Empty<byte>();
    private int _head;
    private int _count;
    private int? _expectedLength;
    private bool _protocolMode;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly CancellationTokenSource _cts = new();
    private Task? _readTask;
    private int _started;
    private volatile bool _disposed;
    // 发送门控：非协议模式时已完成；Reset 时替换为未完成 TCS
    private TaskCompletionSource _syncGate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private CancellationTokenSource? _syncCts;

    public MessageStream(Stream stream, bool enableCRC = true, long maxBufferSize = 256L * 1024 * 1024,
        int syncTimeoutMs = 3000)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _enableCRC = enableCRC;
        _maxBufferSize = maxBufferSize;
        _syncTimeoutMs = syncTimeoutMs;  // 重同步握手超时；<=0 关闭
        _syncGate.SetResult();
    }

    /// <summary>启动读循环。幂等；应在挂好监听器之后调用（RpcProtocol 构造时会自动调用），
    /// 否则读循环可能在监听器注册前就分发早到的消息，造成竞态丢包。</summary>
    public void Start()
    {
        if (_disposed || Interlocked.Exchange(ref _started, 1) == 1) return;
        _readTask = Task.Run(() => ReadLoopAsync(_cts.Token));
    }

    // ========== 发送 ==========

    /// <summary>发送一条消息（带长度头与可选 CRC），并发调用按序串行化。
    /// 协议模式期间门控等待重同步完成（对齐 JS _syncGate）。</summary>
    public async Task SendAsync(byte[] data)
    {
        if (data.Length > _maxBufferSize)
            throw new InvalidOperationException("Data size exceeds maximum allowed limit");

        while (_protocolMode && !_disposed)
            await _syncGate.Task.ConfigureAwait(false);

        await _sendLock.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_disposed) throw new IOException("Stream is not connected");
            var frame = BuildFrame(data);
            await _stream.WriteAsync(frame).ConfigureAwait(false);
            await _stream.FlushAsync().ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private byte[] BuildFrame(byte[] data)
    {
        var lengthHeader = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(lengthHeader, (uint)data.Length);
        if (!_enableCRC)
        {
            var frame = new byte[4 + data.Length];
            lengthHeader.CopyTo(frame, 0);
            data.CopyTo(frame, 4);
            return frame;
        }
        var result = new byte[4 + 4 + data.Length + 4];
        lengthHeader.CopyTo(result, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(4), Crc32(lengthHeader));
        data.CopyTo(result, 8);
        BinaryPrimitives.WriteUInt32LittleEndian(result.AsSpan(8 + data.Length), Crc32(data));
        return result;
    }

    /// <summary>发送错误包（按普通帧格式封装）；不受重同步门控。</summary>
    public async Task SendErrorAsync(string errorType)
    {
        var code = ErrorTypes.GetValueOrDefault(errorType, (byte)0);
        var errorData = new byte[] { 0xEE, 0xEE, 0xEE, 0xEE, code };
        try
        {
            await _sendLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_disposed) throw new IOException("Stream is not connected");
                var frame = BuildFrame(errorData);
                await _stream.WriteAsync(frame).ConfigureAwait(false);
                await _stream.FlushAsync().ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }
        catch (Exception err)
        {
            Console.Error.WriteLine($"Failed to send error packet: {err.Message}");
        }
    }

    /// <summary>发送协议消息: [FFFFFFFF][长度LE][数据]，不带 CRC。</summary>
    public async Task SendProtocolMessageAsync(string message)
    {
        var data = System.Text.Encoding.UTF8.GetBytes(message);
        var frame = new byte[8 + data.Length];
        ProtocolHeader.CopyTo(frame, 0);
        BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(4), (uint)data.Length);
        data.CopyTo(frame, 8);
        try
        {
            await _sendLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_disposed) throw new IOException("Stream is not connected");
                await _stream.WriteAsync(frame).ConfigureAwait(false);
                await _stream.FlushAsync().ConfigureAwait(false);
            }
            finally
            {
                _sendLock.Release();
            }
        }
        catch (Exception err)
        {
            Console.Error.WriteLine($"Failed to send protocol message '{message}': {err.Message}");
        }
    }

    // ========== 缓冲辅助（头游标 + 摊还 compact） ==========

    private ReadOnlySpan<byte> Buf => _buffer.AsSpan(_head, _count);

    private void Append(ReadOnlySpan<byte> data)
    {
        if (_head + _count + data.Length > _buffer.Length)
        {
            int need = _head + _count + data.Length;
            int cap = _buffer.Length == 0 ? 4096 : _buffer.Length;
            while (cap < need) cap *= 2;
            var grown = new byte[cap];
            _buffer.AsSpan(_head, _count).CopyTo(grown);
            _buffer = grown;
            _head = 0;
        }
        data.CopyTo(_buffer.AsSpan(_head + _count));
        _count += data.Length;
    }

    private void Consume(int n)
    {
        _head += n;
        _count -= n;
        if (_head >= 1024 * 1024 || _head >= _count)
        {
            _buffer.AsSpan(_head, _count).CopyTo(_buffer);
            _head = 0;
        }
    }

    private void ClearBuffer()
    {
        _head = 0;
        _count = 0;
    }

    private uint ReadU32(int offset) => BinaryPrimitives.ReadUInt32LittleEndian(Buf.Slice(offset));

    private int FindProtocolHeader()
    {
        int window = Math.Min(_count, 64 * 1024);
        if (window < 4) return -1;
        return Buf.Slice(0, window).IndexOf(ProtocolHeader);
    }

    // ========== 接收 ==========

    private async Task ReadLoopAsync(CancellationToken ct)
    {
        var chunk = new byte[65536];
        try
        {
            while (!ct.IsCancellationRequested)
            {
                int n = await _stream.ReadAsync(chunk, ct).ConfigureAwait(false);
                if (n == 0) break;
                OnData(chunk.AsSpan(0, n));
            }
        }
        catch (OperationCanceledException) { return; }
        catch (Exception err)
        {
            if (!_disposed) Error?.Invoke(new StreamException(err.Message, "ESOCKET"));
        }
        if (!_disposed) Error?.Invoke(new StreamException("Connection closed", "ECLOSED"));
    }

    private void OnData(ReadOnlySpan<byte> data)
    {
        Append(data);

        if (_count > _maxBufferSize)
        {
            Error?.Invoke(new StreamException($"Buffer size: {_count} exceeds maximum allowed limit", "BUFFER_OVERFLOW"));
            _ = SendErrorAsync("BUFFER_OVERFLOW");
            Reset();
            return;
        }

        while (_count > 0)
        {
            // 协议模式：扫描协议头，而不是整段丢弃（避免误丢 RESET_CONFIRM）
            if (_protocolMode)
            {
                int idx = FindProtocolHeader();
                if (idx >= 0)
                {
                    Consume(idx);
                    if (_count < 8) break; // 等待长度字段
                    uint length = ReadU32(4);
                    if (length > 50)
                    {
                        Consume(8); // 伪头：跳过继续扫描
                        continue;
                    }
                    if (_count < 8 + (int)length) break; // 等待协议消息收全
                    var messageData = Buf.Slice(8, (int)length).ToArray();
                    Consume(8 + (int)length);
                    HandleProtocolMessage(messageData);
                    continue;
                }
                // 窗口内无协议头：丢弃已扫描部分，保留尾部 3 字节（可能半截头）
                Consume(_count - Math.Min(3, _count));
                break;
            }

            // 正常模式
            if (_expectedLength is null)
            {
                int headerSize = _enableCRC ? 8 : 4;
                if (_count < headerSize) break;

                uint length = ReadU32(0);
                if (_enableCRC)
                {
                    uint expectedCRC = ReadU32(4);
                    uint calculated = Crc32(Buf.Slice(0, 4));
                    if (calculated != expectedCRC)
                    {
                        Console.Error.WriteLine($"Length header CRC check failed: expected {expectedCRC}, got {calculated}");
                        _ = SendErrorAsync("LENGTH_CRC_FAILED");
                        Error?.Invoke(new StreamException("Length header CRC check failed", "LENGTH_CRC_FAILED"));
                        Reset();
                        return;
                    }
                }

                // 长度头强制校验：0 或超限视为坏帧（先用 uint/long 比较，避免 cast 溢出）
                if (length == 0 || length > (ulong)_maxBufferSize)
                {
                    string code = length > (ulong)_maxBufferSize ? "BUFFER_OVERFLOW" : "LENGTH_CRC_FAILED";
                    _ = SendErrorAsync(code);
                    Error?.Invoke(new StreamException($"invalid frame length {length}", "BAD_LENGTH"));
                    Reset();
                    return;
                }

                _expectedLength = (int)length;
                Consume(headerSize);
            }

            int expected = _expectedLength.Value;
            int messageTotalSize = _enableCRC ? expected + 4 : expected;
            if (_count >= messageTotalSize)
            {
                var messageData = Buf.Slice(0, expected).ToArray();
                if (_enableCRC)
                {
                    uint messageCRC = ReadU32(expected);
                    uint calculated = Crc32(messageData);
                    if (calculated != messageCRC)
                    {
                        Console.Error.WriteLine($"Message CRC check failed: expected {messageCRC}, got {calculated}");
                        _ = SendErrorAsync("MESSAGE_CRC_FAILED");
                        Error?.Invoke(new StreamException("Message CRC check failed", "MESSAGE_CRC_FAILED"));
                        Reset();
                        return;
                    }
                }

                Consume(messageTotalSize);
                _expectedLength = null;

                // 错误包在交给上层之前拦截处理
                if (IsErrorPacket(messageData))
                {
                    var name = ErrorTypeCodes.GetValueOrDefault(messageData[4], "UNKNOWN_ERROR");
                    Console.WriteLine($"Received error packet: {name}");
                    Error?.Invoke(new StreamException($"peer reported {name}", "ERESYNC"));
                    Reset();
                    _ = SendProtocolMessageAsync("RESET_CONFIRM");
                    ExitProtocolMode();
                    continue;
                }

                Message?.Invoke(messageData);
            }
            else break;
        }
    }

    private void HandleProtocolMessage(byte[] messageData)
    {
        var text = System.Text.Encoding.UTF8.GetString(messageData).Trim();
        if (text == "RESET_CONFIRM")
        {
            Console.WriteLine("Received reset confirmation");
            ExitProtocolMode();
            ClearBuffer();
            _expectedLength = null;
            SyncComplete?.Invoke();
        }
    }

    private void ExitProtocolMode()
    {
        _protocolMode = false;
        _syncCts?.Cancel();
        _syncCts = null;
        _syncGate.TrySetResult();
    }

    private async Task SyncWatchAsync(CancellationToken ct)
    {
        try { await Task.Delay(_syncTimeoutMs, ct).ConfigureAwait(false); }
        catch (OperationCanceledException) { return; }
        if (!_protocolMode || _disposed) return;
        ExitProtocolMode();
        Error?.Invoke(new StreamException("resync handshake timeout", "ERESYNC_TIMEOUT"));
        Dispose();
    }

    private static bool IsErrorPacket(byte[] data)
        => data.Length == 5 && data[0] == 0xEE && data[1] == 0xEE && data[2] == 0xEE && data[3] == 0xEE;

    /// <summary>清空接收状态并进入协议模式，等待对端 RESET_CONFIRM。</summary>
    public void Reset()
    {
        Console.WriteLine("[MessageStream] Resetting stream state");
        ClearBuffer();
        _expectedLength = null;
        _protocolMode = true;
        _syncGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _syncCts?.Cancel();
        _syncCts = null;
        if (_syncTimeoutMs > 0)
        {
            _syncCts = new CancellationTokenSource();
            _ = SyncWatchAsync(_syncCts.Token);
        }
    }

    public object GetStatus() => new
    {
        bufferLength = _count,
        expectedLength = _expectedLength,
        enableCRC = _enableCRC,
        protocolMode = _protocolMode,
    };

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _syncCts?.Cancel();
        _syncGate.TrySetResult();  // 唤醒门控中的发送方，使其以 ECLOSED 失败
        _cts.Cancel();
        try { _stream.Dispose(); } catch { /* ignore */ }
        ClearBuffer();
        Message = null;
        Error = null;
        SyncComplete = null;
        Console.WriteLine("[MessageStream] Destroyed");
    }

    // ========== CRC32（与 JS 版一致） ==========

    private static readonly uint[] Crc32Table = BuildCrc32Table();

    private static uint[] BuildCrc32Table()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int j = 0; j < 8; j++)
                c = (c & 1) != 0 ? 0xEDB88320 ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }

    public static uint Crc32(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        foreach (byte b in data)
            crc = Crc32Table[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return crc ^ 0xFFFFFFFF;
    }
}