package ipc;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Java 使用示例（可与其他语言实现互操作）。
 *   服务端: java -cp out ipc.Example server [port]
 *   客户端: java -cp out ipc.Example client <port>
 */
public class Example {

    @SuppressWarnings("unchecked")
    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            System.out.println("usage: Example server [port] | client <port>");
            return;
        }

        if ("server".equals(argv[0])) {
            int port = argv.length > 1 ? Integer.parseInt(argv[1]) : 0;
            ServerSocket serverSocket = new ServerSocket(port, 50, InetAddress.getLoopbackAddress());
            System.out.println("{\"port\": " + serverSocket.getLocalPort() + "}");
            while (true) {
                Socket socket = serverSocket.accept();
                MessageStream stream = new MessageStream(socket.getInputStream(), socket.getOutputStream());
                RpcProtocol rpc = new RpcProtocol(stream);
                rpc.addErrorListener(err -> System.err.println("[java_server] stream error: " + err.getMessage()));

                rpc.registerMethod("add", a -> ((Number) a[0]).longValue() + ((Number) a[1]).longValue());
                rpc.registerMethod("echo", a -> a[0]);
                rpc.registerMethod("greet", a -> "hello " + a[0]);
                rpc.registerMethod("blob", a -> new byte[] { 0x00, 0x01, 0x02, (byte) 0xFF });
                rpc.registerMethod("nothing", a -> null);
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
                rpc.registerMethod("pushEvents", a -> {
                    Map<String, Object> push = new LinkedHashMap<>();
                    push.put("push", 1L);
                    rpc.sendJson(push);
                    rpc.sendText("srv-text");
                    rpc.sendBinary(new byte[] { 1, 2 });
                    return "sent";
                });
            }
        } else if ("client".equals(argv[0])) {
            int port = Integer.parseInt(argv[1]);
            int[] passed = { 0 };
            java.util.function.BiConsumer<String, Boolean> check = (name, cond) -> {
                if (!cond) throw new RuntimeException("FAILED: " + name);
                passed[0]++;
                System.out.println("  ok: " + name);
            };

            Socket socket = new Socket(InetAddress.getLoopbackAddress(), port);
            MessageStream stream = new MessageStream(socket.getInputStream(), socket.getOutputStream());
            RpcProtocol client = new RpcProtocol(stream);
            client.addErrorListener(err -> System.err.println("[java_client] stream error: " + err.getMessage()));
            List<Object[]> events = new ArrayList<>();
            client.onEvent((contentType, content) -> { synchronized (events) { events.add(new Object[] { contentType, content }); } });

            check.accept("json add", ((Number) client.sendRequest("add", Arrays.asList(3, 4)).get(3, TimeUnit.SECONDS)).longValue() == 7);
            Map<String, Object> obj = new LinkedHashMap<>();
            obj.put("name", "测试");
            obj.put("list", Arrays.asList(1L, 2.5, true, null));
            Map<String, Object> echo = (Map<String, Object>) client.sendRequest("echo", Arrays.asList(obj)).get(3, TimeUnit.SECONDS);
            check.accept("json echo unicode", "测试".equals(echo.get("name")) && ((Number) ((List<Object>) echo.get("list")).get(1)).doubleValue() == 2.5);
            check.accept("text response", "hello 世界".equals(client.sendRequest("greet", Arrays.asList("世界")).get(3, TimeUnit.SECONDS)));
            check.accept("binary response", Arrays.equals(new byte[] { 0, 1, 2, (byte) 0xFF },
                    (byte[]) client.sendRequest("blob", null).get(3, TimeUnit.SECONDS)));
            check.accept("null response", client.sendRequest("nothing", null).get(3, TimeUnit.SECONDS) == null);

            try {
                client.sendRequest("fail", null).get(3, TimeUnit.SECONDS);
                check.accept("error response", false);
            } catch (java.util.concurrent.ExecutionException e) {
                check.accept("error response", e.getCause() instanceof RpcProtocol.RpcException
                        && "boom".equals(((RpcProtocol.RpcException) e.getCause()).getError()));
            }

            try {
                client.sendRequest("nope", null).get(3, TimeUnit.SECONDS);
                check.accept("method not found", false);
            } catch (java.util.concurrent.ExecutionException e) {
                check.accept("method not found", String.valueOf(((RpcProtocol.RpcException) e.getCause()).getError()).contains("Method not found: nope"));
            }

            byte[] payload = new byte[256 * 300];
            for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;
            Map<String, Object> up = (Map<String, Object>) client.sendRequestBinary("upload", payload, "tag-1").get(5, TimeUnit.SECONDS);
            check.accept("binary request", ((Number) up.get("size")).longValue() == payload.length
                    && "tag-1".equals(up.get("tag"))
                    && ((Number) ((List<Object>) up.get("head")).get(3)).longValue() == 3);

            check.accept("pushEvents", "sent".equals(client.sendRequest("pushEvents", null).get(3, TimeUnit.SECONDS)));
            Thread.sleep(300);
            synchronized (events) {
                check.accept("events received", events.size() == 3
                        && (int) events.get(0)[0] == RpcProtocol.CONTENT_JSON && ((Map<String, Object>) events.get(0)[1]).get("push").equals(1L)
                        && (int) events.get(1)[0] == RpcProtocol.CONTENT_TEXT && "srv-text".equals(events.get(1)[1])
                        && (int) events.get(2)[0] == RpcProtocol.CONTENT_BINARY && Arrays.equals(new byte[] { 1, 2 }, (byte[]) events.get(2)[1]));
            }

            List<CompletableFuture<Object>> futures = new ArrayList<>();
            for (int i = 0; i < 50; i++) futures.add(client.sendRequest("add", Arrays.asList(i, i)));
            boolean allOk = true;
            for (int i = 0; i < 50; i++)
                allOk &= ((Number) futures.get(i).get(5, TimeUnit.SECONDS)).longValue() == (long) i * 2;
            check.accept("concurrent requests", allOk);

            System.out.println("\nALL " + passed[0] + " JAVA-CLIENT TESTS PASSED");
            client.close();
        } else {
            System.out.println("usage: Example server [port] | client <port>");
        }
    }
}