// C++ 回环测试：g++ -std=c++17 -I../include loopback_test.cpp -o loopback_test -lws2_32
#include <atomic>
#include <chrono>
#include <cstdio>
#include <future>
#include <iostream>
#include <thread>

#include "ipc/json.hpp"
#include "ipc/message_stream.hpp"
#include "ipc/rpc_protocol.hpp"
#include "ipc/tcp.hpp"

using namespace ipc;

static int passed = 0;
static int failed = 0;

static void check(const char* name, bool ok) {
    if (ok) {
        passed++;
        printf("PASS %s\n", name);
    } else {
        failed++;
        printf("FAIL %s\n", name);
    }
}

struct TestServer {
    std::shared_ptr<TcpListener> ln;
    std::shared_ptr<RpcProtocol> rpc;
    std::shared_ptr<std::promise<void>> syncPromise;
    std::thread thread;

    explicit TestServer(std::shared_ptr<std::promise<void>> syncP) : syncPromise(std::move(syncP)) {
        ln = std::make_shared<TcpListener>("127.0.0.1", 0);
        auto ready = std::make_shared<std::promise<void>>();
        readyFuture = ready->get_future();
        thread = std::thread([this, ready] {
            auto conn = ln->accept();
            auto ms = std::make_shared<MessageStream>(conn);
            auto syncP = syncPromise;
            ms->onSyncComplete = [syncP] { syncP->set_value(); };
            rpc = std::make_shared<RpcProtocol>(ms);
            rpc->onError([](const StreamError&) {});
            registerMethods(rpc);
            ready->set_value();
            // 保持服务端对象存活
            std::this_thread::sleep_for(std::chrono::seconds(30));
        });
        thread.detach();
    }

    std::future<void> readyFuture;

    void waitReady() { readyFuture.wait_for(std::chrono::seconds(3)); }

    static void registerMethods(const std::shared_ptr<RpcProtocol>& rpc) {
        rpc->registerMethod("add", [](const std::vector<Json>& a) {
            return Json::ofNum(a[0].asNum() + a[1].asNum());
        });
        rpc->registerMethod("echo", [](const std::vector<Json>& a) { return a[0]; });
        rpc->registerMethod("greet", [](const std::vector<Json>& a) {
            return Json::ofStr("你好, " + a[0].asStr() + "!");
        });
        rpc->registerMethod("blob", [](const std::vector<Json>&) {
            return Json::ofBytes({0, 1, 2, 250, 255});
        });
        rpc->registerMethod("nothing", [](const std::vector<Json>&) { return Json::null(); });
        rpc->registerMethod("fail", [](const std::vector<Json>& a) -> Json {
            throw std::runtime_error("boom: " + a[0].asStr());
        });
        rpc->registerMethod("upload", [](const std::vector<Json>& a) {
            const auto& data = a[0].asBytes();
            Json head = Json::ofArr({});
            for (size_t i = 0; i < 4 && i < data.size(); i++) head.arr.push_back(Json::ofNum(data[i]));
            return Json::object()
                .set("size", Json::ofNum(static_cast<double>(data.size())))
                .set("tag", a[1])
                .set("head", head);
        });
        rpc->registerMethod("pushEvents", [rpc](const std::vector<Json>&) {
            rpc->sendText("text-event");
            rpc->sendJson(Json::object().set("n", Json::ofNum(42)));
            rpc->sendBinary({9, 8, 7});
            return Json::ofStr("sent");
        });
        rpc->registerMethod("slow", [](const std::vector<Json>&) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            return Json::ofStr("late");
        });
    }
};

static std::shared_ptr<RpcProtocol> dial(TestServer& server, std::shared_ptr<TcpStream>& connOut) {
    auto conn = tcpConnect("127.0.0.1", server.ln->port());
    server.waitReady();
    connOut = conn;
    auto rpc = std::make_shared<RpcProtocol>(std::make_shared<MessageStream>(conn));
    rpc->onError([](const StreamError&) {});
    return rpc;
}

// ---- v1.1 辅助：新建一对 rpc 端点（服务端只注册 add） ----
struct RpcPair {
    std::shared_ptr<TcpListener> ln;
    std::shared_ptr<RpcProtocol> server, cli;
    std::shared_ptr<TcpStream> serverConn, cliConn;
};
static std::shared_ptr<RpcPair> makePair(int64_t hbInterval, int64_t hbTimeout,
                                         const std::string& authToken, const std::string& requiredToken);

int main() {
    // 1. JSON 请求 + Unicode
    {
        TestServer server(std::make_shared<std::promise<void>>());
        std::shared_ptr<TcpStream> conn;
        auto rpc = dial(server, conn);
        Json v = rpc->sendRequest("add", Json::ofArr({Json::ofNum(3), Json::ofNum(4)}));
        check("add", v.asNum() == 7);
        Json obj = Json::object().set("x", Json::ofArr({Json::ofNum(1), Json::ofStr("二"), Json::ofBool(true), Json::null()}));
        v = rpc->sendRequest("echo", obj);
        check("echo unicode", v.find("x")->arr[1].asStr() == "二");
        rpc->close();
    }
    // 2. TEXT / BINARY / null 响应
    {
        TestServer server(std::make_shared<std::promise<void>>());
        std::shared_ptr<TcpStream> conn;
        auto rpc = dial(server, conn);
        check("greet", rpc->sendRequest("greet", Json::ofStr("世界")).asStr() == "你好, 世界!");
        Json blob = rpc->sendRequest("blob");
        check("blob", blob.type == Json::Type::Bytes && blob.bytes == std::vector<uint8_t>({0, 1, 2, 250, 255}));
        check("nothing", rpc->sendRequest("nothing").type == Json::Type::Null);
        rpc->close();
    }
    // 3. 错误响应 / 方法不存在
    {
        TestServer server(std::make_shared<std::promise<void>>());
        std::shared_ptr<TcpStream> conn;
        auto rpc = dial(server, conn);
        bool ok = false;
        try {
            rpc->sendRequest("fail", Json::ofStr("x"));
        } catch (const RpcException& e) {
            ok = e.remote && e.error.asStr() == "boom: x";
        }
        check("error response", ok);
        ok = false;
        try {
            rpc->sendRequest("nope");
        } catch (const RpcException& e) {
            ok = e.remote && e.error.asStr() == "Method not found: nope";
        }
        check("method not found", ok);
        rpc->close();
    }
    // 4. 75KB 二进制请求
    {
        TestServer server(std::make_shared<std::promise<void>>());
        std::shared_ptr<TcpStream> conn;
        auto rpc = dial(server, conn);
        std::vector<uint8_t> data(75 * 1024);
        for (size_t i = 0; i < data.size(); i++) data[i] = static_cast<uint8_t>(i % 251);
        Json v = rpc->sendRequestBinary("upload", data, {Json::ofStr("tag-1")});
        check("upload size/tag", v.find("size")->asNum() == static_cast<double>(data.size()) &&
                                   v.find("tag")->asStr() == "tag-1");
        check("upload head", v.find("head")->arr.size() == 4 && v.find("head")->arr[3].asNum() == 3);
        rpc->close();
    }
    // 5. 事件
    {
        TestServer server(std::make_shared<std::promise<void>>());
        std::shared_ptr<TcpStream> conn;
        auto rpc = dial(server, conn);
        std::vector<Json> events;
        std::mutex evMu;
        std::promise<void> evDone;
        auto evFuture = evDone.get_future();
        rpc->onEvent([&](uint8_t, Json content) {
            std::lock_guard<std::mutex> lock(evMu);
            events.push_back(std::move(content));
            if (events.size() == 3) evDone.set_value();
        });
        rpc->sendRequest("pushEvents");
        bool got = evFuture.wait_for(std::chrono::seconds(3)) == std::future_status::ready;
        std::lock_guard<std::mutex> lock(evMu);
        check("events", got && events.size() == 3 && events[0].asStr() == "text-event" &&
                          events[1].find("n")->asNum() == 42 &&
                          events[2].bytes == std::vector<uint8_t>({9, 8, 7}));
        rpc->close();
    }
    // 6. 50 并发
    {
        TestServer server(std::make_shared<std::promise<void>>());
        std::shared_ptr<TcpStream> conn;
        auto rpc = dial(server, conn);
        std::atomic<int> bad{0};
        std::vector<std::thread> threads;
        for (int i = 0; i < 50; i++) {
            threads.emplace_back([rpc, i, &bad] {
                Json v = rpc->sendRequest("add", Json::ofArr({Json::ofNum(i), Json::ofNum(1)}));
                if (v.asNum() != i + 1) bad++;
            });
        }
        for (auto& t : threads) t.join();
        check("concurrent 50", bad.load() == 0);
        rpc->close();
    }
    // 7. CRC 损坏注入 → 错误包 → RESET_CONFIRM → 恢复
    {
        auto syncP = std::make_shared<std::promise<void>>();
        auto syncF = syncP->get_future();
        TestServer server(syncP);
        std::shared_ptr<TcpStream> conn;
        auto rpc = dial(server, conn);

        const char* payload = "hello";
        uint8_t frame[17];
        writeLe32(frame, 5);
        writeLe32(frame + 4, 0xDEADBEEFu);
        memcpy(frame + 8, payload, 5);
        writeLe32(frame + 13, crc32(reinterpret_cast<const uint8_t*>(payload), 5));
        conn->write(frame, sizeof(frame));

        bool synced = syncF.wait_for(std::chrono::seconds(3)) == std::future_status::ready;
        check("syncComplete", synced);
        Json v = rpc->sendRequest("add", Json::ofArr({Json::ofNum(10), Json::ofNum(20)}));
        check("after resync", v.asNum() == 30);
        rpc->close();
    }
    // 8. RPC 超时：显式短超时触发 ETIMEDOUT，pending 清理后迟到响应不影响后续调用
    {
        TestServer server(std::make_shared<std::promise<void>>());
        std::shared_ptr<TcpStream> conn;
        auto rpc = dial(server, conn);
        check("default timeout 15s", rpc->defaultTimeoutMs() == 15000);
        bool ok = false;
        try {
            rpc->sendRequest("slow", Json::null(), "", 100);
        } catch (const RpcException& e) {
            ok = e.code == "ETIMEDOUT" && std::string(e.what()).find("slow") != std::string::npos;
        }
        check("rpc timeout ETIMEDOUT", ok);
        check("after timeout", rpc->sendRequest("add", Json::ofArr({Json::ofNum(1), Json::ofNum(2)})).asNum() == 3);
        std::this_thread::sleep_for(std::chrono::milliseconds(600)); // 迟到响应应被静默丢弃
        check("late response ignored", rpc->sendRequest("add", Json::ofArr({Json::ofNum(5), Json::ofNum(6)})).asNum() == 11);
        rpc->close();
    }
    // 9. 深嵌套 JSON：解析器 64 层上限，超限走 Invalid request payload 路径不崩溃
    {
        // 单元：200 层嵌套直接 parse 应抛异常；64 层以内正常
        std::string deep(200, '[');
        deep.append(200, ']');
        bool threw = false;
        try { Json::parse(deep); } catch (const std::exception&) { threw = true; }
        check("parse depth limit", threw);
        std::string ok64(60, '[');
        ok64.append(60, ']');
        bool parsed = true;
        try { Json::parse(ok64); } catch (const std::exception&) { parsed = false; }
        check("parse within limit", parsed);

        // 端到端：客户端 dump 200 层参数发出，服务端解析失败回错误响应，后续通信正常
        TestServer server(std::make_shared<std::promise<void>>());
        std::shared_ptr<TcpStream> conn;
        auto rpc = dial(server, conn);
        Json nested = Json::ofNum(1);
        for (int i = 0; i < 200; i++) nested = Json::ofArr({nested});
        bool gotError = false;
        try {
            rpc->sendRequest("echo", nested);
        } catch (const RpcException& e) {
            gotError = e.remote && e.error.asStr().find("Invalid request payload") != std::string::npos;
        }
        check("deep frame error response", gotError);
        check("after deep frame", rpc->sendRequest("add", Json::ofArr({Json::ofNum(7), Json::ofNum(8)})).asNum() == 15);
        rpc->close();
    }

    // ---- v1.1 用例（PROTOCOL.md §4.5/§8.1/§8.2） ----

    // 10. HELLO 握手成功：token 匹配 -> ok:true，之后 RPC 正常
    {
        auto p = makePair(0, 0, "secret", "secret");
        Json ack = p->cli->hello();
        check("hello ok", ack.find("ok") && ack.find("ok")->isTruthy() &&
                              ack.find("version")->asNum() == 1);
        check("rpc after hello", p->cli->sendRequest("add", Json::ofArr({Json::ofNum(1), Json::ofNum(2)})).asNum() == 3);
        p->cli->close();
        p->server->close();
    }
    // 11. HELLO 错误 token -> EAUTH，服务端断链
    {
        auto p = makePair(0, 0, "wrong", "secret");
        bool ok = false;
        try {
            p->cli->hello();
        } catch (const RpcException& e) {
            ok = e.code == "EAUTH" && std::string(e.what()) == "Unauthorized";
        }
        check("hello wrong token EAUTH", ok);
        p->cli->close();
        p->server->close();
    }
    // 20. v1.1.3 自动握手（P2）：不显式 hello，首个业务请求也能过鉴权
    {
        auto p = makePair(0, 0, "secret", "secret");
        check("auto hello first request",
              p->cli->sendRequest("add", Json::ofArr({Json::ofNum(2), Json::ofNum(3)})).asNum() == 5);
        check("auto hello authSent", p->cli->authSent());
        p->cli->close();
        p->server->close();
    }
    // 21. 自动握手：并发首请求（8 线程）只握手一次，全部成功
    {
        auto p = makePair(0, 0, "secret", "secret");
        std::vector<std::thread> threads;
        std::vector<double> vals(8, -1);
        for (int i = 0; i < 8; i++) {
            threads.emplace_back([p, i, &vals]() {
                try {
                    vals[i] = p->cli->sendRequest("add", Json::ofArr({Json::ofNum(i), Json::ofNum(1)})).asNum();
                } catch (...) { }
            });
        }
        for (auto& t : threads) t.join();
        bool ok = true;
        for (int i = 0; i < 8; i++) ok = ok && vals[i] == i + 1;
        check("auto hello concurrent one handshake", ok);
        p->cli->close();
        p->server->close();
    }
    // 22. 自动握手：错误 token -> EAUTH
    {
        auto p = makePair(0, 0, "wrong", "secret");
        bool ok = false;
        try {
            p->cli->sendRequest("add", Json::ofArr({Json::ofNum(1), Json::ofNum(1)}));
        } catch (const RpcException& e) {
            ok = e.code == "EAUTH";
        }
        check("auto hello wrong token EAUTH", ok);
        p->cli->close();
        p->server->close();
    }
    // 12. 未握手直接请求 -> Unauthorized 错误响应 + 断链
    {
        auto p = makePair(0, 0, "", "secret");
        bool ok = false;
        try {
            p->cli->sendRequest("add", Json::ofArr({Json::ofNum(1), Json::ofNum(2)}));
        } catch (const RpcException& e) {
            ok = e.remote && e.error.asStr() == "Unauthorized";
        }
        check("unauthorized request", ok);
        p->cli->close();
        p->server->close();
    }
    // 13. 版本不匹配：裸对端回 HELLO_ACK version=99 -> EVERSION
    {
        auto ln = std::make_shared<TcpListener>("127.0.0.1", 0);
        std::thread([ln] {
            auto conn = ln->accept();
            auto ms = std::make_shared<MessageStream>(conn);
            ms->onMessage = [ms](const std::vector<uint8_t>& data) {
                std::vector<uint8_t> msg(18);
                msg[0] = TYPE_HELLO_ACK;
                msg[1] = CONTENT_JSON;
                memcpy(msg.data() + 2, data.data() + 2, 16); // 回显请求 id
                const char* body = "{\"version\":99,\"ok\":true}";
                msg.insert(msg.end(), body, body + strlen(body));
                ms->send(msg);
            };
            ms->start();
            std::this_thread::sleep_for(std::chrono::seconds(30));
        }).detach();
        auto conn = tcpConnect("127.0.0.1", ln->port());
        auto cli = std::make_shared<RpcProtocol>(std::make_shared<MessageStream>(conn), 0, 0, "", "");
        cli->onError([](const StreamError&) {});
        bool ok = false;
        try {
            cli->hello();
        } catch (const RpcException& e) {
            ok = e.code == "EVERSION";
        }
        check("version mismatch EVERSION", ok);
        cli->close();
    }
    // 14. 心跳保活：短间隔 PING/PONG 交换后连接仍可用
    {
        auto p = makePair(100, 5000, "", "");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        check("heartbeat keepalive", p->cli->sendRequest("add", Json::ofArr({Json::ofNum(3), Json::ofNum(4)})).asNum() == 7);
        p->cli->close();
        p->server->close();
    }
    // 15. 心跳超时：静默对端 -> EHEARTBEAT 结算 pending + 断链
    {
        auto ln = std::make_shared<TcpListener>("127.0.0.1", 0);
        std::thread([ln] {
            auto conn = ln->accept(); // 保持连接但不回任何数据
            std::this_thread::sleep_for(std::chrono::seconds(30));
        }).detach();
        auto conn = tcpConnect("127.0.0.1", ln->port());
        auto ms = std::make_shared<MessageStream>(conn);
        auto cli = std::make_shared<RpcProtocol>(ms, 100, 300, "", "");
        cli->onError([](const StreamError&) {});
        bool ok = false;
        try {
            cli->sendRequest("add", Json::ofArr({Json::ofNum(1), Json::ofNum(2)}));
        } catch (const RpcException& e) {
            ok = e.code == "EHEARTBEAT";
        }
        check("heartbeat timeout EHEARTBEAT", ok);
        check("heartbeat closes stream", ms->isClosed());
        cli->close();
    }
    // 16. 重同步超时：坏帧对端发错误包后静默，不回 RESET_CONFIRM -> ERESYNC_TIMEOUT 断链
    {
        auto ln = std::make_shared<TcpListener>("127.0.0.1", 0);
        std::thread([ln] {
            auto conn = ln->accept();
            const char* payload = "hello";
            uint8_t frame[17];
            writeLe32(frame, 5);
            writeLe32(frame + 4, 0xDEADBEEFu);
            memcpy(frame + 8, payload, 5);
            writeLe32(frame + 13, crc32(reinterpret_cast<const uint8_t*>(payload), 5));
            conn->write(frame, sizeof(frame)); // 之后保持静默，不回 RESET_CONFIRM
            std::this_thread::sleep_for(std::chrono::seconds(30));
        }).detach();
        auto conn = tcpConnect("127.0.0.1", ln->port());
        auto ms = std::make_shared<MessageStream>(conn, true, 256 * 1024 * 1024, 300);
        auto cli = std::make_shared<RpcProtocol>(ms, 0, 0, "", "");
        std::mutex codeMu;
        std::vector<std::string> codes;
        cli->onError([&](const StreamError& e) {
            std::lock_guard<std::mutex> lock(codeMu);
            codes.push_back(e.code);
        });
        bool rejected = false;
        try {
            cli->sendRequest("add", Json::ofArr({Json::ofNum(1), Json::ofNum(2)}));
        } catch (const RpcException& e) {
            rejected = e.code == "LENGTH_CRC_FAILED" || e.code == "ERESYNC_TIMEOUT";
        }
        check("resync pending rejected", rejected);
        std::this_thread::sleep_for(std::chrono::milliseconds(500)); // 等满 300ms 重同步计时
        bool gotTimeout = false;
        {
            std::lock_guard<std::mutex> lock(codeMu);
            for (const auto& c : codes) if (c == "ERESYNC_TIMEOUT") gotTimeout = true;
        }
        check("resync timeout ERESYNC_TIMEOUT", gotTimeout);
        check("resync timeout closes stream", ms->isClosed());
        cli->close();
    }

    printf("\n%d passed, %d failed\n", passed, failed);
    // 服务端线程 sleep 30s 会拖延退出，直接结束进程
    std::exit(failed == 0 ? 0 : 1);
}

// ---- v1.1 辅助实现 ----
static std::shared_ptr<RpcPair> makePair(int64_t hbInterval, int64_t hbTimeout,
                                         const std::string& authToken, const std::string& requiredToken) {
    auto p = std::make_shared<RpcPair>();
    p->ln = std::make_shared<TcpListener>("127.0.0.1", 0);
    auto ready = std::make_shared<std::promise<void>>();
    auto fut = ready->get_future();
    std::thread([p, ready, hbInterval, hbTimeout, requiredToken] {
        p->serverConn = p->ln->accept();
        auto ms = std::make_shared<MessageStream>(p->serverConn);
        p->server = std::make_shared<RpcProtocol>(ms, hbInterval, hbTimeout, "", requiredToken);
        p->server->onError([](const StreamError&) {});
        p->server->registerMethod("add", [](const std::vector<Json>& a) {
            return Json::ofNum(a[0].asNum() + a[1].asNum());
        });
        ready->set_value();
        std::this_thread::sleep_for(std::chrono::seconds(30)); // 保持服务端对象存活
    }).detach();
    p->cliConn = tcpConnect("127.0.0.1", p->ln->port());
    auto ms = std::make_shared<MessageStream>(p->cliConn);
    p->cli = std::make_shared<RpcProtocol>(ms, hbInterval, hbTimeout, authToken, "");
    p->cli->onError([](const StreamError&) {});
    fut.wait_for(std::chrono::seconds(3));
    return p;
}