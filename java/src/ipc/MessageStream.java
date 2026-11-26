package ipc;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * MessageStream.js 的 Java 移植版（线格式完全兼容，Java 11+，零依赖）。
 *
 * 帧格式（正常模式，enableCRC=true 时）:
 *   [长度 u32LE][长度头CRC32 u32LE][数据...][数据CRC32 u32LE]
 * 关闭 CRC 时: [长度 u32LE][数据...]
 *
 * 错误包（按普通帧封装，5 字节）: [0xEEEEEEEE][错误类型 1B]
 *   1=LENGTH_CRC_FAILED, 2=MESSAGE_CRC_FAILED, 3=BUFFER_OVERFLOW
 * 重同步协议消息（裸帧，无 CRC）: [0xFFFFFFFF][长度 u32LE][数据(如 'RESET_CONFIRM')]
 *
 * CRC32 使用 java.util.zip.CRC32，与 JS 版（多项式 0xEDB88320）一致。
 * 基于阻塞 IO：调用 start() 后启动守护读线程，send 同步且线程安全。
 * 注意：构造后需先挂好消息监听器再调用 start()（RpcProtocol 会自动处理），
 * 否则读线程可能在监听器注册前就分发早到的消息，造成竞态丢包。
 */
public class MessageStream implements Closeable {

    /** 带错误码的流错误（对齐 JS 修复版 ipcError 的 code 字段）。 */
    public static class StreamException extends IOException {
        private final String code;
        public StreamException(String message, String code) { super(message); this.code = code; }
        public String getCode() { return code; }
    }

    public interface MessageListener { void onMessage(byte[] data); }
    public interface ErrorListener { void onError(Exception err); }
    public interface SyncCompleteListener { void onSyncComplete(); }

    private static final long PROTOCOL_HEADER = 0xFFFFFFFFL;
    private static final long ERROR_HEADER = 0xEEEEEEEEL;

    private static final Map<String, Integer> ERROR_TYPES = new HashMap<>();
    private static final Map<Integer, String> ERROR_TYPE_CODES = new HashMap<>();
    static {
        ERROR_TYPES.put("LENGTH_CRC_FAILED", 1);
        ERROR_TYPES.put("MESSAGE_CRC_FAILED", 2);
        ERROR_TYPES.put("BUFFER_OVERFLOW", 3);
        ERROR_TYPE_CODES.put(1, "LENGTH_CRC_FAILED");
        ERROR_TYPE_CODES.put(2, "MESSAGE_CRC_FAILED");
        ERROR_TYPE_CODES.put(3, "BUFFER_OVERFLOW");
    }

    private final InputStream in;
    private final OutputStream out;
    private final boolean enableCRC;
    private final long maxBufferSize;
    private final long syncTimeoutMs;

    private byte[] buffer = new byte[0];
    private int head = 0;   // 逻辑读游标：消费只前移游标，避免 O(n^2) 全量复制
    private int count = 0;
    private Integer expectedLength = null;
    private boolean protocolMode = false;
    private int syncGeneration = 0;   // 重同步计时世代号，退出协议模式时递增作废旧计时

    private final Object sendLock = new Object();
    private volatile boolean closed = false;
    private Thread readThread;
    private boolean started = false;

    private final List<MessageListener> messageListeners = new ArrayList<>();
    private final List<ErrorListener> errorListeners = new ArrayList<>();
    private final List<SyncCompleteListener> syncListeners = new ArrayList<>();

    public MessageStream(InputStream in, OutputStream out) {
        this(in, out, true, 256L * 1024 * 1024);
    }

    public MessageStream(InputStream in, OutputStream out, boolean enableCRC, long maxBufferSize) {
        this(in, out, enableCRC, maxBufferSize, 3000);
    }

    public MessageStream(InputStream in, OutputStream out, boolean enableCRC, long maxBufferSize, long syncTimeoutMs) {
        if (in == null || out == null) throw new IllegalArgumentException("stream is undefined");
        this.in = in;
        this.out = out;
        this.enableCRC = enableCRC;
        this.maxBufferSize = maxBufferSize;
        this.syncTimeoutMs = syncTimeoutMs;  // 重同步握手超时；<=0 关闭
    }

    /** 启动读循环。幂等；应在挂好监听器之后调用（RpcProtocol 构造时会自动调用）。 */
    public synchronized void start() {
        if (started || closed) return;
        started = true;
        readThread = new Thread(this::readLoop, "MessageStream-reader");
        readThread.setDaemon(true);
        readThread.start();
    }

    // ========== 事件 ==========

    public void addMessageListener(MessageListener l) { synchronized (messageListeners) { messageListeners.add(l); } }
    public void addErrorListener(ErrorListener l) { synchronized (errorListeners) { errorListeners.add(l); } }
    public void addSyncCompleteListener(SyncCompleteListener l) { synchronized (syncListeners) { syncListeners.add(l); } }

    /** 流是否已关闭。 */
    public boolean isClosed() { return closed; }

    private void emitMessage(byte[] data) {
        List<MessageListener> copy;
        synchronized (messageListeners) { copy = new ArrayList<>(messageListeners); }
        for (MessageListener l : copy) {
            try { l.onMessage(data); } catch (Exception e) { System.err.println("[MessageStream] listener error: " + e); }
        }
    }

    private void emitError(Exception err) {
        List<ErrorListener> copy;
        synchronized (errorListeners) { copy = new ArrayList<>(errorListeners); }
        for (ErrorListener l : copy) {
            try { l.onError(err); } catch (Exception e) { System.err.println("[MessageStream] listener error: " + e); }
        }
    }

    private void emitSyncComplete() {
        List<SyncCompleteListener> copy;
        synchronized (syncListeners) { copy = new ArrayList<>(syncListeners); }
        for (SyncCompleteListener l : copy) {
            try { l.onSyncComplete(); } catch (Exception e) { System.err.println("[MessageStream] listener error: " + e); }
        }
    }

    // ========== 发送 ==========

    /** 发送一条消息（带长度头与可选 CRC），并发调用按序串行化。
     * 协议模式期间门控等待重同步完成（对齐 JS _syncGate）。 */
    public void send(byte[] data) throws IOException {
        if (data.length > maxBufferSize)
            throw new IOException("Data size exceeds maximum allowed limit");
        synchronized (this) {
            while (protocolMode && !closed) {
                try { wait(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException("send interrupted"); }
            }
        }
        synchronized (sendLock) {
            if (closed) throw new IOException("Stream is not connected");
            out.write(buildFrame(data));
            out.flush();
        }
    }

    private byte[] buildFrame(byte[] data) {
        byte[] lengthHeader = le32(data.length);
        if (!enableCRC) {
            byte[] frame = new byte[4 + data.length];
            System.arraycopy(lengthHeader, 0, frame, 0, 4);
            System.arraycopy(data, 0, frame, 4, data.length);
            return frame;
        }
        byte[] frame = new byte[8 + data.length + 4];
        System.arraycopy(lengthHeader, 0, frame, 0, 4);
        System.arraycopy(le32((int) crc32(lengthHeader)), 0, frame, 4, 4);
        System.arraycopy(data, 0, frame, 8, data.length);
        System.arraycopy(le32((int) crc32(data)), 0, frame, 8 + data.length, 4);
        return frame;
    }

    /** 发送错误包（按普通帧格式封装）；不受重同步门控。 */
    public void sendError(String errorType) {
        int code = ERROR_TYPES.getOrDefault(errorType, 0);
        byte[] errorData = new byte[] { (byte) 0xEE, (byte) 0xEE, (byte) 0xEE, (byte) 0xEE, (byte) code };
        try {
            synchronized (sendLock) {
                if (closed) throw new IOException("Stream is not connected");
                out.write(buildFrame(errorData));
                out.flush();
            }
        } catch (IOException err) {
            System.err.println("Failed to send error packet: " + err.getMessage());
        }
    }

    /** 发送协议消息: [FFFFFFFF][长度LE][数据]，不带 CRC。 */
    public void sendProtocolMessage(String message) {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        byte[] frame = new byte[8 + data.length];
        System.arraycopy(le32(PROTOCOL_HEADER), 0, frame, 0, 4);
        System.arraycopy(le32(data.length), 0, frame, 4, 4);
        System.arraycopy(data, 0, frame, 8, data.length);
        try {
            synchronized (sendLock) {
                if (closed) throw new IOException("Stream is not connected");
                out.write(frame);
                out.flush();
            }
        } catch (IOException err) {
            System.err.println("Failed to send protocol message '" + message + "': " + err.getMessage());
        }
    }

    // ========== 缓冲辅助（头游标 + 摊还 compact） ==========

    private int bufLen() { return count; }

    private void append(byte[] data, int n) {
        if (head + count + n > buffer.length) {
            int need = head + count + n;
            int cap = buffer.length == 0 ? 4096 : buffer.length;
            while (cap < need) cap *= 2;
            byte[] grown = new byte[cap];
            System.arraycopy(buffer, head, grown, 0, count);
            buffer = grown;
            head = 0;
        }
        System.arraycopy(data, 0, buffer, head + count, n);
        count += n;
    }

    private void consume(int n) {
        head += n;
        count -= n;
        if (head >= 1024 * 1024 || head >= count) {
            System.arraycopy(buffer, head, buffer, 0, count);
            head = 0;
        }
    }

    private void clearBuffer() { head = 0; count = 0; }

    private long readU32(int offset) { return readLe32(buffer, head + offset); }

    private byte[] take(int n, int offset) { return Arrays.copyOfRange(buffer, head + offset, head + offset + n); }

    private int findProtocolHeader() {
        int window = Math.min(count, 64 * 1024);
        for (int i = 0; i + 4 <= window; i++) {
            if (readLe32(buffer, head + i) == PROTOCOL_HEADER) return i;
        }
        return -1;
    }

    // ========== 接收 ==========

    private void readLoop() {
        byte[] chunk = new byte[65536];
        try {
            while (!closed) {
                int n = in.read(chunk);
                if (n < 0) break;
                synchronized (this) {
                    onData(chunk, n);
                }
            }
        } catch (IOException err) {
            if (!closed) emitError(new StreamException(err.getMessage(), "ESOCKET"));
        }
        if (!closed) emitError(new StreamException("Connection closed", "ECLOSED"));
    }

    private void onData(byte[] data, int n) {
        append(data, n);

        if (bufLen() > maxBufferSize) {
            emitError(new StreamException("Buffer size: " + bufLen() + " exceeds maximum allowed limit", "BUFFER_OVERFLOW"));
            sendError("BUFFER_OVERFLOW");
            reset();
            return;
        }

        while (bufLen() > 0) {
            // 协议模式：扫描协议头，而不是整段丢弃（避免误丢 RESET_CONFIRM）
            if (protocolMode) {
                int idx = findProtocolHeader();
                if (idx >= 0) {
                    consume(idx);
                    if (bufLen() < 8) break; // 等待长度字段
                    long length = readU32(4);
                    if (length > 50) {
                        consume(8); // 伪头：跳过继续扫描
                        continue;
                    }
                    if (bufLen() < 8 + length) break; // 等待协议消息收全
                    byte[] messageData = take((int) length, 8);
                    consume(8 + (int) length);
                    handleProtocolMessage(messageData);
                    continue;
                }
                // 窗口内无协议头：丢弃已扫描部分，保留尾部 3 字节（可能半截头）
                consume(bufLen() - Math.min(3, bufLen()));
                break;
            }

            // 正常模式
            if (expectedLength == null) {
                int headerSize = enableCRC ? 8 : 4;
                if (bufLen() < headerSize) break;

                long length = readU32(0);
                if (enableCRC) {
                    long expectedCRC = readU32(4);
                    long calculated = crc32(take(4, 0));
                    if (calculated != expectedCRC) {
                        System.err.println("Length header CRC check failed: expected " + expectedCRC + ", got " + calculated);
                        sendError("LENGTH_CRC_FAILED");
                        emitError(new StreamException("Length header CRC check failed", "LENGTH_CRC_FAILED"));
                        reset();
                        return;
                    }
                }

                // 长度头强制校验：0 或超限视为坏帧（先用 long 比较，避免 cast 溢出为负）
                if (length == 0 || length > maxBufferSize) {
                    String code = length > maxBufferSize ? "BUFFER_OVERFLOW" : "LENGTH_CRC_FAILED";
                    sendError(code);
                    emitError(new StreamException("invalid frame length " + length, "BAD_LENGTH"));
                    reset();
                    return;
                }

                expectedLength = (int) length;
                consume(headerSize);
            }

            int messageTotalSize = enableCRC ? expectedLength + 4 : expectedLength;
            if (bufLen() >= messageTotalSize) {
                byte[] messageData = take(expectedLength, 0);
                if (enableCRC) {
                    long messageCRC = readU32(expectedLength);
                    long calculated = crc32(messageData);
                    if (calculated != messageCRC) {
                        System.err.println("Message CRC check failed: expected " + messageCRC + ", got " + calculated);
                        sendError("MESSAGE_CRC_FAILED");
                        emitError(new StreamException("Message CRC check failed", "MESSAGE_CRC_FAILED"));
                        reset();
                        return;
                    }
                }

                consume(messageTotalSize);
                expectedLength = null;

                // 错误包在交给上层之前拦截处理
                if (isErrorPacket(messageData)) {
                    String name = ERROR_TYPE_CODES.getOrDefault(messageData[4] & 0xFF, "UNKNOWN_ERROR");
                    System.out.println("Received error packet: " + name);
                    emitError(new StreamException("peer reported " + name, "ERESYNC"));
                    reset();
                    sendProtocolMessage("RESET_CONFIRM");
                    exitProtocolMode();
                    continue;
                }

                emitMessage(messageData);
            } else {
                break;
            }
        }
    }

    private void handleProtocolMessage(byte[] messageData) {
        String text = new String(messageData, StandardCharsets.UTF_8).trim();
        if ("RESET_CONFIRM".equals(text)) {
            System.out.println("Received reset confirmation");
            exitProtocolMode();
            clearBuffer();
            expectedLength = null;
            emitSyncComplete();
        }
    }

    /** 退出协议模式：作废旧计时并唤醒门控中的发送方。调用方需持有 this  monitor。 */
    private void exitProtocolMode() {
        protocolMode = false;
        syncGeneration++;
        notifyAll();
    }

    private void syncWatch(int gen) {
        try { Thread.sleep(syncTimeoutMs); }
        catch (InterruptedException e) { return; }
        synchronized (this) {
            if (gen != syncGeneration || !protocolMode || closed) return;
            exitProtocolMode();
        }
        emitError(new StreamException("resync handshake timeout", "ERESYNC_TIMEOUT"));
        close();
    }

    private static boolean isErrorPacket(byte[] data) {
        return data.length == 5 && readLe32(data, 0) == ERROR_HEADER;
    }

    /** 清空接收状态并进入协议模式，等待对端 RESET_CONFIRM。 */
    public synchronized void reset() {
        System.out.println("[MessageStream] Resetting stream state");
        clearBuffer();
        expectedLength = null;
        protocolMode = true;
        final int gen = ++syncGeneration;
        if (syncTimeoutMs > 0) {
            Thread t = new Thread(() -> syncWatch(gen), "MessageStream-syncwatch");
            t.setDaemon(true);
            t.start();
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        synchronized (this) {
            syncGeneration++;
            notifyAll();  // 唤醒门控中的发送方，使其以 ECLOSED 失败
        }
        try { in.close(); } catch (IOException ignored) {}
        try { out.close(); } catch (IOException ignored) {}
        synchronized (this) {
            clearBuffer();
            expectedLength = null;
        }
        synchronized (messageListeners) { messageListeners.clear(); }
        synchronized (errorListeners) { errorListeners.clear(); }
        synchronized (syncListeners) { syncListeners.clear(); }
        System.out.println("[MessageStream] Destroyed");
    }

    // ========== 工具 ==========

    public static long crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    static byte[] le32(long v) {
        return new byte[] { (byte) v, (byte) (v >>> 8), (byte) (v >>> 16), (byte) (v >>> 24) };
    }

    static long readLe32(byte[] b, int off) {
        return (b[off] & 0xFFL) | ((b[off + 1] & 0xFFL) << 8)
                | ((b[off + 2] & 0xFFL) << 16) | ((b[off + 3] & 0xFFL) << 24);
    }

    static byte[] concat(byte[] a, byte[] b) {
        byte[] result = new byte[a.length + b.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }
}