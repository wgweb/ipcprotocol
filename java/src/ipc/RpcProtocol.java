package ipc;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * RpcProtocol.js 的 Java 移植版（线格式完全兼容，Java 11+，零依赖）。
 *
 * 消息格式: [type u8][contentType u8][UUID 16B][payload...]
 *   type:        0=REQUEST, 1=RESPONSE, 2=EVENT, 3=PING, 4=PONG, 5=HELLO, 6=HELLO_ACK
 *   contentType: 0=TEXT(utf8), 1=JSON(utf8), 2=BINARY(原始字节)
 * JSON 请求体: {"method":...,"params":...,"to":...(可选)}；错误响应体: {"error":...}
 * 二进制请求体: [methodLen u32BE][method utf8][paramsLen u32BE][params JSON utf8][binary...]
 *   服务端 handler 收到的参数为 [binary, ...params]
 *
 * JSON 值映射: Map&lt;String,Object&gt; / List&lt;Object&gt; / String / Long / Double / Boolean / null。
 */
public class RpcProtocol implements Closeable {

    public static final int TYPE_REQUEST = 0, TYPE_RESPONSE = 1, TYPE_EVENT = 2;
    public static final int TYPE_PING = 3, TYPE_PONG = 4, TYPE_HELLO = 5, TYPE_HELLO_ACK = 6;
    public static final int CONTENT_TEXT = 0, CONTENT_JSON = 1, CONTENT_BINARY = 2;
    /** 线协议版本（HELLO 协商用）。 */
    public static final long PROTOCOL_VERSION = 1;

    /** 方法处理器，返回值映射: null->JSON null, String->TEXT, byte[]->BINARY, 其他->JSON。 */
    public interface Handler { Object handle(Object[] args) throws Exception; }

    public interface EventListener { void onEvent(int contentType, Object content); }
    public interface ErrorListener { void onError(Exception err); }

    /** 对端返回的错误响应，getError() 携带原始 error 值；getCode() 为错误码（如 ETIMEDOUT/EREMOTE）。 */
    public static class RpcException extends RuntimeException {
        private final Object error;
        private final String code;
        public RpcException(Object error) { this(error, null); }
        public RpcException(Object error, String code) {
            super(String.valueOf(error));
            this.error = error;
            this.code = code;
        }
        public Object getError() { return error; }
        public String getCode() { return code; }
    }

    private MessageStream messageStream;
    private final Map<String, Handler> handlers = new ConcurrentHashMap<>();
    private final Map<String, CompletableFuture<Object>> pendingRequests = new ConcurrentHashMap<>();
    private final List<EventListener> eventListeners = new ArrayList<>();
    private final List<ErrorListener> errorListeners = new ArrayList<>();
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "RpcProtocol-worker");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "RpcProtocol-timeout");
        t.setDaemon(true);
        return t;
    });
    /** 默认 RPC 超时（毫秒），对齐 JS 修复版 15s；0 表示关闭。 */
    private volatile long defaultTimeoutMs = 15000;
    private volatile boolean closed = false;

    // ---- v1.1: 心跳与 HELLO 握手（对齐 PROTOCOL.md §8.1/§8.2） ----
    private final long heartbeatIntervalMs;
    private final long heartbeatTimeoutMs;
    private final String authToken;
    /** v1.1.3：首个业务请求前自动握手的并发闸门。 */
    private volatile boolean authSent = false;
    private volatile CompletableFuture<Void> authFuture = null;
    private final String requiredToken;
    private volatile long lastReceivedMs;
    private volatile boolean authenticated;
    private int unauthorizedHits = 0;
    private ScheduledFuture<?> heartbeatTask;

    // ---- v1.1.2: 能力协商（PROTOCOL.md §8.4） ----
    public static final List<String> DEFAULT_CAPABILITIES = List.of("ping", "binary", "event");
    private final List<String> capabilities;
    private volatile String implName = "java";
    private volatile List<String> peerCapabilities = null;
    private volatile List<String> serverCapabilities = null;
    private volatile String heartbeatDisabledReason = null;

    public RpcProtocol(MessageStream messageStream) {
        this(messageStream, 5000, 15000, null, null);
    }

    /**
     * @param heartbeatIntervalMs 心跳发送间隔（毫秒），<=0 关闭心跳
     * @param heartbeatTimeoutMs  静默超时（毫秒），超时以 EHEARTBEAT 断链；<=0 关闭
     * @param authToken           客户端 HELLO 携带的鉴权令牌（可选；为空时回退环境变量 IPC_TOKEN）
     * @param requiredToken       服务端要求的令牌；非 null 时未通过 HELLO 鉴权的请求被拒绝
     */
    public RpcProtocol(MessageStream messageStream, long heartbeatIntervalMs, long heartbeatTimeoutMs,
                       String authToken, String requiredToken) {
        this(messageStream, heartbeatIntervalMs, heartbeatTimeoutMs, authToken, requiredToken,
                DEFAULT_CAPABILITIES);
    }

    public RpcProtocol(MessageStream messageStream, long heartbeatIntervalMs, long heartbeatTimeoutMs,
                       String authToken, String requiredToken, List<String> capabilities) {
        if (messageStream == null) throw new IllegalArgumentException("messageStream is undefined");
        this.messageStream = messageStream;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        String envToken = System.getenv("IPC_TOKEN");
        this.authToken = (authToken != null && !authToken.isEmpty()) ? authToken
                : (envToken != null && !envToken.isEmpty() ? envToken : null);
        this.requiredToken = requiredToken;
        this.capabilities = (capabilities == null || capabilities.isEmpty())
                ? DEFAULT_CAPABILITIES : List.copyOf(capabilities);
        // v1.1.3：区分服务端校验位与客户端握手完成，否则客户端会把自己当作已鉴权而跳过 HELLO
        this.authenticated = (requiredToken == null && this.authToken == null);
        this.lastReceivedMs = System.currentTimeMillis();
        messageStream.addMessageListener(this::handleMessage);
        messageStream.addErrorListener(err -> {
            handleStreamError(err);
            emitError(err);
        });
        // 先挂好监听器再启动读循环，避免早到消息在注册前被分发的竞态
        messageStream.start();
        if (heartbeatIntervalMs > 0) {
            heartbeatTask = timeoutScheduler.scheduleWithFixedDelay(
                    this::heartbeatTick, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    public List<String> getCapabilities() { return capabilities; }
    public String getImplName() { return implName; }
    public void setImplName(String name) { implName = name; }
    public List<String> getPeerCapabilities() { return peerCapabilities; }
    public List<String> getServerCapabilities() { return serverCapabilities; }
    public String getHeartbeatDisabledReason() { return heartbeatDisabledReason; }
    public boolean isAuthenticated() { return authenticated; }

    /** 对端声明了能力但不含 ping：停发心跳并停用静默超时，避免误杀。 */
    private void disableHeartbeat(String reason) {
        if (heartbeatDisabledReason != null) return;
        heartbeatDisabledReason = reason;
        ScheduledFuture<?> task = heartbeatTask;
        if (task != null) task.cancel(false);
        heartbeatTask = null;
    }

    private List<String> readCapabilities(Object content) {
        if (!(content instanceof Map)) return null;
        Object caps = ((Map<?, ?>) content).get("caps");
        if (!(caps instanceof List)) return null;
        List<String> out = new ArrayList<>();
        for (Object item : (List<?>) caps) if (item instanceof String) out.add((String) item);
        return out;
    }

    /** 心跳节拍：静默超过 timeout 则以 EHEARTBEAT 结算全部 pending 并断链；否则按间隔发 PING。 */
    private void heartbeatTick() {
        if (closed || heartbeatDisabledReason != null) return;
        long silent = System.currentTimeMillis() - lastReceivedMs;
        if (heartbeatTimeoutMs > 0 && silent >= heartbeatTimeoutMs) {
            RpcException err = new RpcException("heartbeat timeout: no data for " + silent + "ms", "EHEARTBEAT");
            rejectAll(err);
            emitError(err);
            close();
            return;
        }
        if (silent >= heartbeatIntervalMs) {
            try { sendRaw(TYPE_PING, CONTENT_TEXT, UUID.randomUUID().toString(), ""); }
            catch (IOException ignored) { }
        }
    }

    private void rejectAll(RpcException err) {
        for (String id : new ArrayList<>(pendingRequests.keySet())) {
            CompletableFuture<Object> pending = pendingRequests.remove(id);
            if (pending != null) pending.completeExceptionally(err);
        }
    }

    // ========== 事件 ==========

    public void onEvent(EventListener listener) { synchronized (eventListeners) { eventListeners.add(listener); } }
    public void addErrorListener(ErrorListener listener) { synchronized (errorListeners) { errorListeners.add(listener); } }

    public long getDefaultTimeoutMs() { return defaultTimeoutMs; }
    public void setDefaultTimeoutMs(long ms) { defaultTimeoutMs = ms; }

    private void emitEvent(int contentType, Object content) {
        List<EventListener> copy;
        synchronized (eventListeners) { copy = new ArrayList<>(eventListeners); }
        for (EventListener l : copy) {
            try { l.onEvent(contentType, content); } catch (Exception e) { System.err.println("[RpcProtocol] listener error: " + e); }
        }
    }

    private void emitError(Exception err) {
        List<ErrorListener> copy;
        synchronized (errorListeners) { copy = new ArrayList<>(errorListeners); }
        for (ErrorListener l : copy) {
            try { l.onError(err); } catch (Exception e) { System.err.println("[RpcProtocol] listener error: " + e); }
        }
    }

    private void handleStreamError(Exception error) {
        String message = error.getMessage() != null ? error.getMessage() : "Connection error";
        String code = error instanceof MessageStream.StreamException
                ? ((MessageStream.StreamException) error).getCode() : null;
        for (String id : new ArrayList<>(pendingRequests.keySet())) {
            CompletableFuture<Object> pending = pendingRequests.remove(id);
            if (pending != null) pending.completeExceptionally(new RpcException("Request failed: " + message, code));
        }
    }

    // ========== 序列化 ==========

    public static byte[] serialize(int type, int contentType, String id, Object data) throws IOException {
        byte[] header = new byte[18];
        header[0] = (byte) type;
        header[1] = (byte) contentType;
        UUID uuid = UUID.fromString(id);
        ByteBuffer.wrap(header, 2, 16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());

        byte[] payload;
        if (contentType == CONTENT_TEXT) {
            payload = ((String) data).getBytes(StandardCharsets.UTF_8);
        } else if (contentType == CONTENT_JSON) {
            payload = Json.stringify(data).getBytes(StandardCharsets.UTF_8);
        } else if (contentType == CONTENT_BINARY) {
            payload = (byte[]) data;
        } else {
            throw new IOException("Unsupported content type: " + contentType);
        }
        return MessageStream.concat(header, payload);
    }

    public static class Message {
        public final int type, contentType;
        public final String id;
        public final Object content;
        Message(int type, int contentType, String id, Object content) {
            this.type = type; this.contentType = contentType; this.id = id; this.content = content;
        }
    }

    public static Message deserialize(byte[] buffer) throws IOException {
        if (buffer.length < 18) throw new IOException("Failed to deserialize message: buffer too short");
        int type = buffer[0] & 0xFF;
        int contentType = buffer[1] & 0xFF;
        ByteBuffer bb = ByteBuffer.wrap(buffer, 2, 16);
        String id = new UUID(bb.getLong(), bb.getLong()).toString();
        byte[] payload = Arrays.copyOfRange(buffer, 18, buffer.length);
        Object content;
        if (contentType == CONTENT_TEXT) {
            content = new String(payload, StandardCharsets.UTF_8);
        } else if (contentType == CONTENT_JSON) {
            content = Json.parse(new String(payload, StandardCharsets.UTF_8));
        } else if (contentType == CONTENT_BINARY) {
            content = payload;
        } else {
            throw new IOException("Failed to deserialize message: unknown content type " + contentType);
        }
        return new Message(type, contentType, id, content);
    }

    public static byte[] encodeBinaryRequest(String method, List<Object> params, byte[] binaryData) {
        byte[] methodBuf = method.getBytes(StandardCharsets.UTF_8);
        byte[] paramsBuf = Json.stringify(params).getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[4 + methodBuf.length + 4 + paramsBuf.length + binaryData.length];
        int offset = 0;
        System.arraycopy(be32(methodBuf.length), 0, result, offset, 4); offset += 4;
        System.arraycopy(methodBuf, 0, result, offset, methodBuf.length); offset += methodBuf.length;
        System.arraycopy(be32(paramsBuf.length), 0, result, offset, 4); offset += 4;
        System.arraycopy(paramsBuf, 0, result, offset, paramsBuf.length); offset += paramsBuf.length;
        System.arraycopy(binaryData, 0, result, offset, binaryData.length);
        return result;
    }

    public static class BinaryRequest {
        public final String method;
        public final List<Object> params;
        public final byte[] binaryData;
        BinaryRequest(String method, List<Object> params, byte[] binaryData) {
            this.method = method; this.params = params; this.binaryData = binaryData;
        }
    }

    @SuppressWarnings("unchecked")
    public static BinaryRequest decodeBinaryRequest(byte[] buffer) {
        int offset = 0;
        if (buffer.length < 4) throw new IllegalArgumentException("binary request too short");
        long methodLength = readBe32(buffer, offset); offset += 4;
        if (buffer.length < offset + methodLength + 4) throw new IllegalArgumentException("binary request truncated (method)");
        String method = new String(buffer, offset, (int) methodLength, StandardCharsets.UTF_8); offset += (int) methodLength;
        long paramsLength = readBe32(buffer, offset); offset += 4;
        if (buffer.length < offset + paramsLength) throw new IllegalArgumentException("binary request truncated (params)");
        Object parsed = Json.parse(new String(buffer, offset, (int) paramsLength, StandardCharsets.UTF_8)); offset += (int) paramsLength;
        List<Object> params = parsed instanceof List ? (List<Object>) parsed : new ArrayList<>(Arrays.asList(parsed));
        byte[] binaryData = Arrays.copyOfRange(buffer, offset, buffer.length);
        return new BinaryRequest(method, params, binaryData);
    }

    // ========== 接收处理 ==========

    private void handleMessage(byte[] buffer) {
        try {
            Message msg = deserialize(buffer);
            if (msg.type < 0 || msg.type > 6) throw new IOException("Invalid message type: " + msg.type);
            if (msg.contentType < 0 || msg.contentType > 2) throw new IOException("Invalid content type: " + msg.contentType);
            lastReceivedMs = System.currentTimeMillis();   // 任何入站消息都刷新心跳基准

            if (msg.type == TYPE_REQUEST) {
                if (requiredToken != null && !authenticated) { rejectUnauthorized(msg.id); return; }
                executor.submit(() -> handleRequest(msg.id, msg.content));
            } else if (msg.type == TYPE_RESPONSE) {
                handleResponse(msg.id, msg.content);
            } else if (msg.type == TYPE_EVENT) {
                if (requiredToken != null && !authenticated) return;   // 未鉴权事件直接丢弃
                emitEvent(msg.contentType, msg.content);
            } else if (msg.type == TYPE_PING) {
                sendPong(msg.id);
            } else if (msg.type == TYPE_PONG) {
                // 仅刷新 lastReceived（上面已做）
            } else if (msg.type == TYPE_HELLO) {
                handleHello(msg.id, msg.content);
            } else {
                handleHelloAck(msg.id, msg.content);
            }
        } catch (Exception err) {
            System.err.println("Failed to handle message: " + err.getMessage());
            // 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版）
            if (buffer.length >= 18 && (buffer[0] & 0xFF) == TYPE_REQUEST) {
                try {
                    ByteBuffer bb = ByteBuffer.wrap(buffer, 2, 16);
                    String id = new UUID(bb.getLong(), bb.getLong()).toString();
                    Map<String, Object> error = new LinkedHashMap<>();
                    error.put("error", "Invalid request payload");
                    sendResponse(id, error, CONTENT_JSON);
                } catch (Exception ignored) { }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void handleRequest(String id, Object request) {
        String method;
        Object[] args;

        try {
            if (request instanceof byte[]) {
                BinaryRequest decoded = decodeBinaryRequest((byte[]) request);
                method = decoded.method;
                List<Object> list = new ArrayList<>();
                list.add(decoded.binaryData);
                list.addAll(decoded.params);
                args = list.toArray();
            } else if (request instanceof Map) {
                Map<String, Object> map = (Map<String, Object>) request;
                Object m = map.get("method");
                method = m instanceof String ? (String) m : null;
                Object params = map.get("params");
                if (params instanceof List) {
                    args = ((List<Object>) params).toArray();
                } else if (params == null) {
                    args = new Object[0];
                } else {
                    args = new Object[] { params };
                }
            } else {
                throw new IllegalArgumentException("request payload is not an object");
            }
            if (method == null || method.isEmpty()) {
                throw new IllegalArgumentException("request method missing");
            }
        } catch (Exception err) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", "Invalid request payload: " + err.getMessage());
            sendResponse(id, error, CONTENT_JSON);
            return;
        }

        Handler handler = handlers.get(method);
        if (handler == null) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", "Method not found: " + method);
            sendResponse(id, error, CONTENT_JSON);
            return;
        }

        try {
            Object result = handler.handle(args);
            int contentType = CONTENT_JSON;
            if (result instanceof String) contentType = CONTENT_TEXT;
            else if (result instanceof byte[]) contentType = CONTENT_BINARY;
            sendResponse(id, result, contentType);
        } catch (Exception err) {
            Map<String, Object> error = new LinkedHashMap<>();
            error.put("error", err.getMessage() != null ? err.getMessage() : err.toString());
            sendResponse(id, error, CONTENT_JSON);
        }
    }

    @SuppressWarnings("unchecked")
    private void handleResponse(String id, Object response) {
        CompletableFuture<Object> pending = pendingRequests.remove(id);
        if (pending == null) return;
        if (response instanceof Map && isTruthy(((Map<String, Object>) response).get("error"))) {
            pending.completeExceptionally(new RpcException(((Map<String, Object>) response).get("error"), "EREMOTE"));
        } else {
            pending.complete(response);
        }
    }

    // JS 真值语义：null/false/0/"" 为假
    private static boolean isTruthy(Object v) {
        if (v == null || Boolean.FALSE.equals(v)) return false;
        if (v instanceof String) return !((String) v).isEmpty();
        if (v instanceof Number) return ((Number) v).doubleValue() != 0;
        return true;
    }

    // ========== v1.1: 心跳与 HELLO 握手 ==========

    private void sendPong(String id) {
        try { sendRaw(TYPE_PONG, CONTENT_TEXT, id, ""); }
        catch (IOException err) { System.err.println("Failed to send pong: " + err.getMessage()); }
    }

    /** 未鉴权 REQUEST：回 Unauthorized 错误响应后断链（对齐 PROTOCOL.md §8.2）。 */
    private void rejectUnauthorized(String id) {
        Map<String, Object> error = new LinkedHashMap<>();
        error.put("error", "Unauthorized");
        sendResponse(id, error, CONTENT_JSON);
        // v1.1.2：回错误但不断链（允许随后 HELLO 重试）；连续 5 次才断开
        if (++unauthorizedHits >= 5) close();
    }

    @SuppressWarnings("unchecked")
    private void handleHello(String id, Object content) {
        Map<String, Object> ack = new LinkedHashMap<>();
        ack.put("version", PROTOCOL_VERSION);
        boolean ok = false;
        if (content instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) content;
            Object version = map.get("version");
            if (!(version instanceof Number) || ((Number) version).longValue() != PROTOCOL_VERSION) {
                ack.put("error", "Unsupported protocol version");
            } else if (requiredToken != null && !requiredToken.equals(map.get("token"))) {
                ack.put("error", "Unauthorized");
            } else {
                authenticated = true;
                peerCapabilities = readCapabilities(map);
                if (peerCapabilities != null && !peerCapabilities.contains("ping")) {
                    disableHeartbeat("peer " + implName + " declares no 'ping' capability");
                }
                ack.put("ok", true);
                ack.put("caps", capabilities);
                ack.put("impl", implName);
                ok = true;
            }
        } else {
            ack.put("error", "Invalid hello payload");
        }
        try { sendRaw(TYPE_HELLO_ACK, CONTENT_JSON, id, ack); }
        catch (IOException err) { System.err.println("Failed to send hello ack: " + err.getMessage()); }
        if (!ok) close();   // 握手失败后服务端断链
    }

    @SuppressWarnings("unchecked")
    private void handleHelloAck(String id, Object content) {
        CompletableFuture<Object> pending = pendingRequests.remove(id);
        if (pending == null) return;
        if (!(content instanceof Map)) {
            pending.completeExceptionally(new RpcException("Invalid hello ack payload", "EAUTH"));
            return;
        }
        Map<String, Object> map = (Map<String, Object>) content;
        if (isTruthy(map.get("error"))) {
            pending.completeExceptionally(new RpcException(map.get("error"), "EAUTH"));
            return;
        }
        Object version = map.get("version");
        if (!(version instanceof Number) || ((Number) version).longValue() != PROTOCOL_VERSION) {
            pending.completeExceptionally(new RpcException("Unsupported protocol version", "EVERSION"));
            return;
        }
        serverCapabilities = readCapabilities(map);
        if (serverCapabilities != null && !serverCapabilities.contains("ping")) {
            disableHeartbeat("server declares no 'ping' capability");
        }
        authenticated = true;
        pending.complete(content);
    }

    /**
     * 首个业务请求前自动握手（v1.1.3，P2）。
     * 仅客户端（requiredToken == null 且持有 authToken）生效；并发调用共享同一个握手 Future。
     */
    private synchronized CompletableFuture<Void> ensureAuth() {
        if (requiredToken != null || authToken == null) return CompletableFuture.completedFuture(null);
        if (authenticated || authSent) return CompletableFuture.completedFuture(null);
        if (authFuture != null) return authFuture;
        CompletableFuture<Void> f = hello(defaultTimeoutMs).thenApply(ignored -> {
            authSent = true;
            return null;
        });
        authFuture = f.whenComplete((v, err) -> { if (err != null) authFuture = null; });
        return authFuture;
    }

    /** 是否已完成（或被跳过）自动握手。 */
    public boolean isAuthSent() { return authSent || authenticated; }

    /** 发起 HELLO 握手（默认超时）；ACK error → EAUTH，版本不符 → EVERSION。 */
    public CompletableFuture<Object> hello() {
        return hello(defaultTimeoutMs);
    }

    public CompletableFuture<Object> hello(long timeoutMs) {
        String id = UUID.randomUUID().toString();
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.put(id, future);
        armTimeout(id, future, "hello", timeoutMs);
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("version", PROTOCOL_VERSION);
        payload.put("caps", capabilities);
        payload.put("impl", implName);
        if (authToken != null) payload.put("token", authToken);
        try {
            sendRaw(TYPE_HELLO, CONTENT_JSON, id, payload);
        } catch (IOException err) {
            pendingRequests.remove(id);
            future.completeExceptionally(err);
        }
        return future;
    }

    // ========== 发送 ==========

    public void sendRaw(int type, int contentType, String id, Object data) throws IOException {
        if (messageStream == null) throw new IOException("messageStream is not initialized");
        messageStream.send(serialize(type, contentType, id, data));
    }

    /** 发送 JSON 请求并等待响应。params 为 List 时展开为多参数。 */
    public CompletableFuture<Object> sendRequest(String method, Object params) {
        return sendRequest(method, params, null);
    }

    public CompletableFuture<Object> sendRequest(String method, Object params, String to) {
        return sendRequest(method, params, to, defaultTimeoutMs);
    }

    /** 带显式超时（毫秒）的请求；0 关闭超时。超时以 RpcException(code="ETIMEDOUT") 完成，对齐 JS 修复版。 */
    public CompletableFuture<Object> sendRequest(String method, Object params, String to, long timeoutMs) {
        return ensureAuth().thenCompose(ignored -> doSendRequest(method, params, to, timeoutMs));
    }

    private CompletableFuture<Object> doSendRequest(String method, Object params, String to, long timeoutMs) {
        String id = UUID.randomUUID().toString();
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.put(id, future);
        armTimeout(id, future, method, timeoutMs);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("method", method);
        body.put("params", params);
        if (to != null) body.put("to", to);
        try {
            sendRaw(TYPE_REQUEST, CONTENT_JSON, id, body);
        } catch (IOException err) {
            pendingRequests.remove(id);
            future.completeExceptionally(err);
        }
        return future;
    }

    /** 发送二进制请求并等待响应（默认超时）。服务端 handler 收到 [data, ...args]。 */
    public CompletableFuture<Object> sendRequestBinary(String method, byte[] data, Object... args) {
        return sendRequestBinaryWithTimeout(defaultTimeoutMs, method, data, args);
    }

    /** 带显式超时（毫秒）的二进制请求；0 关闭超时。 */
    public CompletableFuture<Object> sendRequestBinaryWithTimeout(long timeoutMs, String method, byte[] data, Object... args) {
        return ensureAuth().thenCompose(ignored -> doSendRequestBinary(timeoutMs, method, data, args));
    }

    private CompletableFuture<Object> doSendRequestBinary(long timeoutMs, String method, byte[] data, Object... args) {
        String id = UUID.randomUUID().toString();
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingRequests.put(id, future);
        armTimeout(id, future, method, timeoutMs);
        byte[] binaryRequest = encodeBinaryRequest(method, Arrays.asList(args), data);
        try {
            sendRaw(TYPE_REQUEST, CONTENT_BINARY, id, binaryRequest);
        } catch (IOException err) {
            pendingRequests.remove(id);
            future.completeExceptionally(err);
        }
        return future;
    }

    /** 超时触发：先移除 pending 再拒绝，避免迟到响应写入已废弃的 future。 */
    private void armTimeout(String id, CompletableFuture<Object> future, String method, long timeoutMs) {
        if (timeoutMs <= 0) return;
        ScheduledFuture<?> timer = timeoutScheduler.schedule(() -> {
            if (pendingRequests.remove(id) != null) {
                future.completeExceptionally(new RpcException(
                        "RPC timeout after " + timeoutMs + "ms: " + method, "ETIMEDOUT"));
            }
        }, timeoutMs, TimeUnit.MILLISECONDS);
        future.whenComplete((r, e) -> timer.cancel(false));
    }

    public void sendResponse(String id, Object response, int contentType) {
        try {
            sendRaw(TYPE_RESPONSE, contentType, id, response);
        } catch (IOException err) {
            System.err.println("Failed to send response: " + err.getMessage());
        }
    }

    // ========== 辅助方法 ==========

    public void sendText(String text) {
        try { sendRaw(TYPE_EVENT, CONTENT_TEXT, UUID.randomUUID().toString(), text); }
        catch (IOException err) { System.err.println("Failed to send text event: " + err.getMessage()); }
    }

    public void sendBinary(byte[] data) {
        try { sendRaw(TYPE_EVENT, CONTENT_BINARY, UUID.randomUUID().toString(), data); }
        catch (IOException err) { System.err.println("Failed to send binary event: " + err.getMessage()); }
    }

    public void sendJson(Object data) {
        try { sendRaw(TYPE_EVENT, CONTENT_JSON, UUID.randomUUID().toString(), data); }
        catch (IOException err) { System.err.println("Failed to send json event: " + err.getMessage()); }
    }

    public void registerMethod(String name, Handler handler) {
        handlers.put(name, handler);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (heartbeatTask != null) heartbeatTask.cancel(false);
        for (String id : new ArrayList<>(pendingRequests.keySet())) {
            CompletableFuture<Object> pending = pendingRequests.remove(id);
            if (pending != null) pending.completeExceptionally(new RpcException("RPC Protocol destroyed"));
        }
        if (messageStream != null) {
            messageStream.close();
            messageStream = null;
        }
        handlers.clear();
        synchronized (eventListeners) { eventListeners.clear(); }
        synchronized (errorListeners) { errorListeners.clear(); }
        executor.shutdownNow();
        timeoutScheduler.shutdownNow();
        System.out.println("[RpcProtocol] Destroyed");
    }

    // ========== 工具 ==========

    static byte[] be32(long v) {
        return new byte[] { (byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v };
    }

    static long readBe32(byte[] b, int off) {
        return ((b[off] & 0xFFL) << 24) | ((b[off + 1] & 0xFFL) << 16)
                | ((b[off + 2] & 0xFFL) << 8) | (b[off + 3] & 0xFFL);
    }
}