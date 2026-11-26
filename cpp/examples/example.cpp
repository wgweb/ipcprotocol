// C++ 示例：server/client 双模式，用于与其他语言实现互操作。
// g++ -std=c++17 -O2 -I../include example.cpp -o example.exe -lws2_32
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <iostream>
#include <thread>

#include "ipc/json.hpp"
#include "ipc/message_stream.hpp"
#include "ipc/rpc_protocol.hpp"
#include "ipc/tcp.hpp"

using namespace ipc;

static void registerMethods(const std::shared_ptr<RpcProtocol>& rpc) {
    rpc->registerMethod("add", [](const std::vector<Json>& a) {
        return Json::ofNum(a[0].asNum() + a[1].asNum());
    });
    rpc->registerMethod("echo", [](const std::vector<Json>& a) { return a[0]; });
    rpc->registerMethod("greet", [](const std::vector<Json>& a) {
        return Json::ofStr("hello " + a[0].asStr());
    });
    rpc->registerMethod("blob", [](const std::vector<Json>&) {
        return Json::ofBytes({0, 1, 2, 255});
    });
    rpc->registerMethod("nothing", [](const std::vector<Json>&) { return Json::null(); });
    rpc->registerMethod("fail", [](const std::vector<Json>&) -> Json {
        throw std::runtime_error("boom");
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
        rpc->sendJson(Json::object().set("push", Json::ofNum(1)));
        rpc->sendText("srv-text");
        rpc->sendBinary({1, 2});
        return Json::ofStr("sent");
    });
}

static void runServer(uint16_t port) {
    TcpListener ln("127.0.0.1", port);
    printf("{\"port\":%u}\n", ln.port());
    fflush(stdout);
    for (;;) {
        auto conn = ln.accept();
        std::thread([conn] {
            auto rpc = std::make_shared<RpcProtocol>(std::make_shared<MessageStream>(conn));
            rpc->onError([](const StreamError&) {});
            registerMethods(rpc);
            // 保持连接存活
            std::this_thread::sleep_for(std::chrono::hours(24));
        }).detach();
    }
}

static int passed = 0;

static void check(const char* name, bool ok) {
    if (ok) {
        passed++;
        printf("PASS %s\n", name);
    } else {
        printf("FAIL %s\n", name);
        std::exit(1);
    }
}

static void runClient(uint16_t port) {
    auto conn = tcpConnect("127.0.0.1", port);
    auto rpc = std::make_shared<RpcProtocol>(std::make_shared<MessageStream>(conn));
    rpc->onError([](const StreamError&) {});

    check("add", rpc->sendRequest("add", Json::ofArr({Json::ofNum(3), Json::ofNum(4)})).asNum() == 7);

    Json obj = Json::object()
                   .set("name", Json::ofStr("测试"))
                   .set("list", Json::ofArr({Json::ofNum(1), Json::ofNum(2.5), Json::ofBool(true), Json::null()}));
    Json echo = rpc->sendRequest("echo", obj);
    check("echo", echo.find("name")->asStr() == "测试" && echo.find("list")->arr[1].asNum() == 2.5);

    check("greet", rpc->sendRequest("greet", Json::ofStr("世界")).asStr() == "hello 世界");

    Json blob = rpc->sendRequest("blob");
    check("blob", blob.type == Json::Type::Bytes && blob.bytes == std::vector<uint8_t>({0, 1, 2, 255}));

    check("nothing", rpc->sendRequest("nothing").type == Json::Type::Null);

    bool ok = false;
    try {
        rpc->sendRequest("fail");
    } catch (const RpcException& e) {
        ok = e.remote && e.error.asStr() == "boom";
    }
    check("fail", ok);

    ok = false;
    try {
        rpc->sendRequest("nope");
    } catch (const RpcException& e) {
        ok = e.remote && e.error.asStr() == "Method not found: nope";
    }
    check("methodNotFound", ok);

    std::vector<uint8_t> data(256 * 300);
    for (size_t i = 0; i < data.size(); i++) data[i] = static_cast<uint8_t>(i & 0xFF);
    Json up = rpc->sendRequestBinary("upload", data, {Json::ofStr("tag-1")});
    check("upload", up.find("size")->asNum() == static_cast<double>(data.size()) &&
                        up.find("tag")->asStr() == "tag-1" && up.find("head")->arr[3].asNum() == 3);

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
    {
        std::lock_guard<std::mutex> lock(evMu);
        check("events", got && events.size() == 3 && events[0].find("push")->asNum() == 1 &&
                              events[1].asStr() == "srv-text" &&
                              events[2].bytes == std::vector<uint8_t>({1, 2}));
    }

    std::atomic<int> bad{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 50; i++) {
        threads.emplace_back([rpc, i, &bad] {
            Json v = rpc->sendRequest("add", Json::ofArr({Json::ofNum(i), Json::ofNum(i)}));
            if (v.asNum() != i * 2) bad++;
        });
    }
    for (auto& t : threads) t.join();
    check("concurrent50", bad.load() == 0);

    check("toField", rpc->sendRequest("echo", Json::ofStr("routed"), "some-target").asStr() == "routed");

    printf("ALL %d CHECKS PASSED\n", passed);
    rpc->close();
    std::exit(0);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: example server|client <port>\n");
        return 2;
    }
    uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
    if (std::string(argv[1]) == "server") runServer(port);
    else if (std::string(argv[1]) == "client") runClient(port);
    else return 2;
    return 0;
}