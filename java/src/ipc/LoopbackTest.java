package ipc;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Java 版回环测试：TCP 服务端 + 客户端在同一进程内验证全部协议路径。 */
public class LoopbackTest {

    static int passed = 0;

    static void check(String name, boolean cond) {
        if (!cond) throw new RuntimeException("FAILED: " + name);
        passed++;
        System.out.println("  ok: " + name);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        int port = serverSocket.getLocalPort();

        final MessageStream[] serverStreamHolder = new MessageStream[1];
        CountDownLatch serverReady = new CountDownLatch(1);
        Thread serverThread = new Thread(() -> {
            try {
                Socket socket = serverSocket.accept();
                MessageStream stream = new MessageStream(socket.getInputStream(), socket.getOutputStream());
                serverStreamHolder[0] = stream;
                RpcProtocol rpc = new RpcProtocol(stream);
                rpc.registerMethod("add", a -> ((Number) a[0]).longValue() + ((Number) a[1]).longValue());
                rpc.registerMethod("echo", a -> a[0]);
                rpc.registerMethod("greet", a -> "hello " + a[0]);                       // String -> TEXT
                rpc.registerMethod("blob", a -> new byte[] { 0x00, 0x01, 0x02, (byte) 0xFF }); // byte[] -> BINARY
                rpc.registerMethod("nothing", a -> null);                                // null -> JSON null
                rpc.registerMethod("fail", a -> { throw new Exception("boom"); });
                rpc.registerMethod("upload", a -> {
                    byte[] data = (byte[]) a[0];
                    Map<String, Object> result = new LinkedHashMap<>();
                    result.put("size", (long) data.length);
                    result.put("tag", a[1]);
                    List<Object> head = new ArrayList<>();
                    for (int i = 0; i < 4; i++) head.add((long) (data[i] & 0xFF));
                    result.put("head", head);
                    return result;
                });
                rpc.registerMethod("slow", a -> { Thread.sleep(500); return "late"; });
                serverReady.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        Socket clientSocket = new Socket(InetAddress.getLoopbackAddress(), port);
        InputStream cin = clientSocket.getInputStream();
        OutputStream cout = clientSocket.getOutputStream();
        MessageStream clientStream = new MessageStream(cin, cout);
        RpcProtocol client = new RpcProtocol(clientStream);
        serverReady.await();

        List<Object[]> events = new ArrayList<>();
        client.onEvent((contentType, content) -> { synchronized (events) { events.add(new Object[] { contentType, content }); } });

        // 1. JSON 请求/响应
        check("json add", ((Number) client.sendRequest("add", Arrays.asList(3, 4)).get(3, TimeUnit.SECONDS)).longValue() == 7);
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("name", "测试");
        obj.put("list", Arrays.asList(1L, 2.5, true, null));
        Map<String, Object> echo = (Map<String, Object>) client.sendRequest("echo", Arrays.asList(obj)).get(3, TimeUnit.SECONDS);
        check("json echo unicode", "测试".equals(echo.get("name")) && ((Number) ((List<Object>) echo.get("list")).get(1)).doubleValue() == 2.5);

        // 2. TEXT 响应
        check("text response", "hello 世界".equals(client.sendRequest("greet", Arrays.asList("世界")).get(3, TimeUnit.SECONDS)));

        // 3. BINARY 响应
        check("binary response", Arrays.equals(new byte[] { 0, 1, 2, (byte) 0xFF },
                (byte[]) client.sendRequest("blob", null).get(3, TimeUnit.SECONDS)));

        // 4. null 响应
        check("null response", client.sendRequest("nothing", null).get(3, TimeUnit.SECONDS) == null);

        // 5. 错误响应
        try {
            client.sendRequest("fail", null).get(3, TimeUnit.SECONDS);
            check("error response", false);
        } catch (java.util.concurrent.ExecutionException e) {
            check("error response", e.getCause() instanceof RpcProtocol.RpcException
                    && "boom".equals(((RpcProtocol.RpcException) e.getCause()).getError()));
        }

        // 6. 方法不存在
        try {
            client.sendRequest("nope", null).get(3, TimeUnit.SECONDS);
            check("method not found", false);
        } catch (java.util.concurrent.ExecutionException e) {
            check("method not found", String.valueOf(((RpcProtocol.RpcException) e.getCause()).getError()).contains("Method not found: nope"));
        }

        // 7. 二进制请求（~75KB，跨多个 TCP 分段）
        byte[] payload = new byte[256 * 300];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;
        Map<String, Object> up = (Map<String, Object>) client.sendRequestBinary("upload", payload, "tag-1").get(5, TimeUnit.SECONDS);
        check("binary request", ((Number) up.get("size")).longValue() == payload.length
                && "tag-1".equals(up.get("tag"))
                && ((Number) ((List<Object>) up.get("head")).get(3)).longValue() == 3);

        // 8. 事件（TEXT/JSON/BINARY）：服务端 -> 客户端
        MessageStream srv = serverStreamHolder[0];
        Map<String, Object> push = new LinkedHashMap<>();
        push.put("push", 1L);
        srv.send(RpcProtocol.serialize(RpcProtocol.TYPE_EVENT, RpcProtocol.CONTENT_JSON, UUID.randomUUID().toString(), push));
        srv.send(RpcProtocol.serialize(RpcProtocol.TYPE_EVENT, RpcProtocol.CONTENT_TEXT, UUID.randomUUID().toString(), "srv-text"));
        srv.send(RpcProtocol.serialize(RpcProtocol.TYPE_EVENT, RpcProtocol.CONTENT_BINARY, UUID.randomUUID().toString(), new byte[] { 1, 2 }));
        Thread.sleep(300);
        synchronized (events) {
            check("events received", events.size() == 3
                    && (int) events.get(0)[0] == RpcProtocol.CONTENT_JSON && ((Map<String, Object>) events.get(0)[1]).get("push").equals(1L)
                    && (int) events.get(1)[0] == RpcProtocol.CONTENT_TEXT && "srv-text".equals(events.get(1)[1])
                    && (int) events.get(2)[0] == RpcProtocol.CONTENT_BINARY && Arrays.equals(new byte[] { 1, 2 }, (byte[]) events.get(2)[1]));
        }

        // 9. 并发请求
        List<CompletableFuture<Object>> futures = new ArrayList<>();
        for (int i = 0; i < 50; i++) futures.add(client.sendRequest("add", Arrays.asList(i, i)));
        boolean allOk = true;
        for (int i = 0; i < 50; i++)
            allOk &= ((Number) futures.get(i).get(5, TimeUnit.SECONDS)).longValue() == (long) i * 2;
        check("concurrent requests", allOk);

        // 10. CRC 错误恢复：客户端注入长度头 CRC 损坏的帧 -> 服务端检测错误并回错误包
        //     -> 客户端回 RESET_CONFIRM -> 服务端 syncComplete，之后通信应正常
        List<Exception> srvErrors = new ArrayList<>();
        srv.addErrorListener(srvErrors::add);
        CountDownLatch syncDone = new CountDownLatch(1);
        srv.addSyncCompleteListener(syncDone::countDown);

        byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] bad = MessageStream.concat(
                MessageStream.concat(MessageStream.le32(5), MessageStream.le32(0xDEADBEEFL)),
                MessageStream.concat(hello, MessageStream.le32(MessageStream.crc32(hello))));
        cout.write(bad);
        cout.flush();
        check("resync completed", syncDone.await(3, TimeUnit.SECONDS));
        check("crc error detected", !srvErrors.isEmpty());
        check("resync then rpc works", ((Number) client.sendRequest("add", Arrays.asList(10, 20)).get(3, TimeUnit.SECONDS)).longValue() == 30);

        // 11. RPC 超时：显式短超时触发 ETIMEDOUT，pending 清理后迟到响应不影响后续调用
        try {
            client.sendRequest("slow", null, null, 100).get(3, TimeUnit.SECONDS);
            check("rpc timeout ETIMEDOUT", false);
        } catch (java.util.concurrent.ExecutionException e) {
            RpcProtocol.RpcException re = (RpcProtocol.RpcException) e.getCause();
            check("rpc timeout ETIMEDOUT", "ETIMEDOUT".equals(re.getCode()) && String.valueOf(re.getError()).contains("slow"));
        }
        check("after timeout rpc works", ((Number) client.sendRequest("add", Arrays.asList(1, 2)).get(3, TimeUnit.SECONDS)).longValue() == 3);
        Thread.sleep(600); // 等 slow 的迟到响应到达（应被静默丢弃）
        check("late response ignored", ((Number) client.sendRequest("add", Arrays.asList(5, 6)).get(3, TimeUnit.SECONDS)).longValue() == 11);
        check("default timeout is 15s", client.getDefaultTimeoutMs() == 15000);

        // ---- v1.1 用例（PROTOCOL.md §4.5/§8.1/§8.2） ----

        // 12. HELLO 握手成功：token 匹配 -> ok:true，之后 RPC 正常
        Pair p12 = pair(0, 0, "secret", "secret");
        Map<String, Object> ack12 = (Map<String, Object>) p12.cli.hello().get(3, TimeUnit.SECONDS);
        check("hello ok", Boolean.TRUE.equals(ack12.get("ok")) && ((Number) ack12.get("version")).longValue() == 1);
        check("rpc after hello", ((Number) p12.cli.sendRequest("add", Arrays.asList(1, 2)).get(3, TimeUnit.SECONDS)).longValue() == 3);
        p12.cli.close(); p12.server.close(); p12.ln.close();

        // 13. HELLO 错误 token -> EAUTH，服务端断链
        Pair p13 = pair(0, 0, "wrong", "secret");
        try {
            p13.cli.hello().get(3, TimeUnit.SECONDS);
            check("hello wrong token EAUTH", false);
        } catch (java.util.concurrent.ExecutionException e) {
            RpcProtocol.RpcException re = (RpcProtocol.RpcException) e.getCause();
            check("hello wrong token EAUTH", "EAUTH".equals(re.getCode()) && "Unauthorized".equals(re.getError()));
        }
        p13.cli.close(); p13.server.close(); p13.ln.close();

        // 14. 未握手直接请求 -> Unauthorized 错误响应 + 断链
        Pair p14 = pair(0, 0, null, "secret");
        try {
            p14.cli.sendRequest("add", Arrays.asList(1, 2)).get(3, TimeUnit.SECONDS);
            check("unauthorized request", false);
        } catch (java.util.concurrent.ExecutionException e) {
            RpcProtocol.RpcException re = (RpcProtocol.RpcException) e.getCause();
            check("unauthorized request", "Unauthorized".equals(re.getError()));
        }
        p14.cli.close(); p14.server.close(); p14.ln.close();

        // 15. 版本不匹配：裸对端回 HELLO_ACK version=99 -> EVERSION
        ServerSocket ln15 = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread t15 = new Thread(() -> {
            try {
                Socket s = ln15.accept();
                synchronized (keepAlive) { keepAlive.add(s); }
                MessageStream ms = new MessageStream(s.getInputStream(), s.getOutputStream());
                ms.addMessageListener(buf -> {
                    try {
                        RpcProtocol.Message m = RpcProtocol.deserialize(buf);
                        Map<String, Object> ack = new LinkedHashMap<>();
                        ack.put("version", 99L);
                        ack.put("ok", true);
                        ms.send(RpcProtocol.serialize(RpcProtocol.TYPE_HELLO_ACK, RpcProtocol.CONTENT_JSON, m.id, ack));
                    } catch (Exception ignored) { }
                });
                ms.start();
            } catch (Exception ignored) { }
        });
        t15.setDaemon(true);
        t15.start();
        Socket cs15 = new Socket(InetAddress.getLoopbackAddress(), ln15.getLocalPort());
        RpcProtocol cli15 = new RpcProtocol(new MessageStream(cs15.getInputStream(), cs15.getOutputStream()), 0, 0, null, null);
        try {
            cli15.hello().get(3, TimeUnit.SECONDS);
            check("version mismatch EVERSION", false);
        } catch (java.util.concurrent.ExecutionException e) {
            check("version mismatch EVERSION", "EVERSION".equals(((RpcProtocol.RpcException) e.getCause()).getCode()));
        }
        cli15.close(); ln15.close();

        // 16. 心跳保活：短间隔 PING/PONG 交换后连接仍可用
        Pair p16 = pair(100, 5000, null, null);
        Thread.sleep(500);
        check("heartbeat keepalive", ((Number) p16.cli.sendRequest("add", Arrays.asList(3, 4)).get(3, TimeUnit.SECONDS)).longValue() == 7);
        p16.cli.close(); p16.server.close(); p16.ln.close();

        // 17. 心跳超时：静默对端 -> EHEARTBEAT 结算 pending + 断链
        ServerSocket ln17 = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread t17 = new Thread(() -> {
            try {
                Socket s = ln17.accept();
                synchronized (keepAlive) { keepAlive.add(s); }   // 保持连接但不回任何数据
            } catch (Exception ignored) { }
        });
        t17.setDaemon(true);
        t17.start();
        Socket cs17 = new Socket(InetAddress.getLoopbackAddress(), ln17.getLocalPort());
        MessageStream ms17 = new MessageStream(cs17.getInputStream(), cs17.getOutputStream());
        RpcProtocol cli17 = new RpcProtocol(ms17, 100, 300, null, null);
        CompletableFuture<Object> f17 = cli17.sendRequest("add", Arrays.asList(1, 2));
        try {
            f17.get(3, TimeUnit.SECONDS);
            check("heartbeat timeout EHEARTBEAT", false);
        } catch (java.util.concurrent.ExecutionException e) {
            check("heartbeat timeout EHEARTBEAT", "EHEARTBEAT".equals(((RpcProtocol.RpcException) e.getCause()).getCode()));
        }
        check("heartbeat closes stream", ms17.isClosed());
        cli17.close(); ln17.close();

        // 18. 重同步超时：坏帧对端发错误包后静默，不回 RESET_CONFIRM -> ERESYNC_TIMEOUT 断链
        ServerSocket ln18 = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread t18 = new Thread(() -> {
            try {
                Socket s = ln18.accept();
                synchronized (keepAlive) { keepAlive.add(s); }
                byte[] hb = "hello".getBytes(StandardCharsets.UTF_8);
                byte[] badFrame = MessageStream.concat(
                        MessageStream.concat(MessageStream.le32(5), MessageStream.le32(0xDEADBEEFL)),
                        MessageStream.concat(hb, MessageStream.le32(MessageStream.crc32(hb))));
                s.getOutputStream().write(badFrame);
                s.getOutputStream().flush();   // 之后保持静默，不回 RESET_CONFIRM
            } catch (Exception ignored) { }
        });
        t18.setDaemon(true);
        t18.start();
        Socket cs18 = new Socket(InetAddress.getLoopbackAddress(), ln18.getLocalPort());
        MessageStream ms18 = new MessageStream(cs18.getInputStream(), cs18.getOutputStream(), true, 256L * 1024 * 1024, 300);
        RpcProtocol cli18 = new RpcProtocol(ms18, 0, 0, null, null);
        List<String> codes18 = new ArrayList<>();
        cli18.addErrorListener(err -> {
            if (err instanceof MessageStream.StreamException) codes18.add(((MessageStream.StreamException) err).getCode());
        });
        CompletableFuture<Object> f18 = cli18.sendRequest("add", Arrays.asList(1, 2));
        try {
            f18.get(3, TimeUnit.SECONDS);
            check("resync pending rejected", false);
        } catch (java.util.concurrent.ExecutionException e) {
            String code = ((RpcProtocol.RpcException) e.getCause()).getCode();
            check("resync pending rejected", "LENGTH_CRC_FAILED".equals(code) || "ERESYNC_TIMEOUT".equals(code));
        }
        Thread.sleep(500);   // 等满 300ms 重同步计时
        check("resync timeout ERESYNC_TIMEOUT", codes18.contains("ERESYNC_TIMEOUT"));
        check("resync timeout closes stream", ms18.isClosed());
        cli18.close(); ln18.close();

        // 20. v1.1.3 自动 hello（P2）：客户端不显式 hello，首个业务请求也能过鉴权
        Pair pA = pair(0, 0, "tok-auto", "tok-auto");
        check("auto hello: first request authorized",
                ((Number) pA.cli.sendRequest("add", Arrays.asList(2, 3)).get(3, TimeUnit.SECONDS)).longValue() == 5);
        check("auto hello: auth sent flag", pA.cli.isAuthSent());
        pA.cli.close();

        Pair pB = pair(0, 0, "tok-auto", "tok-auto");
        List<CompletableFuture<Object>> autoFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) autoFutures.add(pB.cli.sendRequest("add", Arrays.asList(i, 1)));
        boolean autoAllOk = true;
        for (int i = 0; i < 5; i++) {
            autoAllOk &= ((Number) autoFutures.get(i).get(3, TimeUnit.SECONDS)).longValue() == i + 1;
        }
        check("auto hello: concurrent first requests only one handshake", autoAllOk);
        pB.cli.close();

        Pair pC = pair(0, 0, "bad-token", "tok-auto");
        try {
            pC.cli.sendRequest("add", Arrays.asList(1, 1)).get(3, TimeUnit.SECONDS);
            check("auto hello: wrong token rejected", false);
        } catch (java.util.concurrent.ExecutionException e) {
            check("auto hello: wrong token rejected",
                    "EAUTH".equals(((RpcProtocol.RpcException) e.getCause()).getCode()));
        }
        pC.cli.close();

        // 19. destroy 后资源清理
        client.close();
        serverSocket.close();
        System.out.println("\nALL " + passed + " JAVA TESTS PASSED");
    }

    // ---- v1.1 辅助：新建一对 rpc 端点 ----
    static final List<Object> keepAlive = new ArrayList<>();   // 防止静默对端 socket 被 GC 关闭

    static class Pair {
        ServerSocket ln; Socket cs; RpcProtocol server; RpcProtocol cli;
    }

    @SuppressWarnings("unchecked")
    static Pair pair(long hbInterval, long hbTimeout, String authToken, String requiredToken) throws Exception {
        Pair p = new Pair();
        p.ln = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        CountDownLatch ready = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try {
                Socket s = p.ln.accept();
                synchronized (keepAlive) { keepAlive.add(s); }
                MessageStream ms = new MessageStream(s.getInputStream(), s.getOutputStream());
                p.server = new RpcProtocol(ms, hbInterval, hbTimeout, null, requiredToken);
                p.server.registerMethod("add", a -> ((Number) a[0]).longValue() + ((Number) a[1]).longValue());
                ready.countDown();
            } catch (Exception ignored) { }
        });
        t.setDaemon(true);
        t.start();
        p.cs = new Socket(InetAddress.getLoopbackAddress(), p.ln.getLocalPort());
        MessageStream cms = new MessageStream(p.cs.getInputStream(), p.cs.getOutputStream());
        p.cli = new RpcProtocol(cms, hbInterval, hbTimeout, authToken, null);
        ready.await();
        return p;
    }
}