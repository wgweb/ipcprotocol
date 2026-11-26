// RpcProtocol 消息层：请求/响应/事件，与 RpcProtocol.js 线格式兼容。
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "json.hpp"
#include "message_stream.hpp"

namespace ipc {

constexpr uint8_t TYPE_REQUEST = 0;
constexpr uint8_t TYPE_RESPONSE = 1;
constexpr uint8_t TYPE_EVENT = 2;
constexpr uint8_t TYPE_PING = 3;
constexpr uint8_t TYPE_PONG = 4;
constexpr uint8_t TYPE_HELLO = 5;
constexpr uint8_t TYPE_HELLO_ACK = 6;

constexpr uint8_t CONTENT_TEXT = 0;
constexpr uint8_t CONTENT_JSON = 1;
constexpr uint8_t CONTENT_BINARY = 2;

// 线协议版本（HELLO 协商用）。
constexpr double PROTOCOL_VERSION = 1.0;

inline int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// RPC 错误：remote=true 时 error 携带对端原始 error 值（对齐 JS reject(response.error)）；
// code 为错误码（如 ETIMEDOUT/EREMOTE）。
class RpcException : public std::runtime_error {
public:
    Json error;
    bool remote;
    std::string code;

    explicit RpcException(const std::string& msg, std::string code = "")
        : std::runtime_error(msg), remote(false), code(std::move(code)) {}
    explicit RpcException(Json remoteValue)
        : std::runtime_error("rpc error: " + remoteValue.dump()), error(std::move(remoteValue)), remote(true), code("EREMOTE") {}
};

// 服务端方法处理器：抛 std::runtime_error 转为 {"error": ...} 响应。
using Handler = std::function<Json(const std::vector<Json>&)>;

class RpcProtocol {
public:
    // 包装消息流（默认心跳 5s/15s，不鉴权）：先注册回调再启动读循环，避免早到消息丢失。
    explicit RpcProtocol(std::shared_ptr<MessageStream> ms)
        : RpcProtocol(std::move(ms), 5000, 15000, "", "") {}

    // heartbeatIntervalMs <=0 关闭心跳；requiredToken 非空时未通过 HELLO 鉴权的请求被拒绝。
    RpcProtocol(std::shared_ptr<MessageStream> ms, int64_t heartbeatIntervalMs, int64_t heartbeatTimeoutMs,
                std::string authToken, std::string requiredToken)
        : state_(std::make_shared<State>()), authToken_(std::move(authToken)), ms_(std::move(ms)) {
        state_->requiredToken = std::move(requiredToken);
        // v1.1.2：未显式传 token 时回退环境变量 IPC_TOKEN（Node 主进程注入）
        if (authToken_.empty()) {
            if (const char* env = std::getenv("IPC_TOKEN")) {
                if (*env) authToken_ = env;
            }
        }
        // v1.1.3：区分服务端校验位与客户端握手完成，否则客户端会把自己当作已鉴权而跳过 HELLO
        state_->authenticated.store(state_->requiredToken.empty() && authToken_.empty());
        state_->lastReceived.store(nowMs());
        auto state = state_;
        auto msCopy = ms_;
        ms_->onMessage = [state, msCopy](const std::vector<uint8_t>& data) { handleMessage(data, state, msCopy); };
        ms_->onError = [state](const StreamError& e) {
            rejectAll(state, "Request failed: " + e.message, e.code);
            std::lock_guard<std::mutex> lock(state->mu);
            if (state->onError) state->onError(e);
        };
        ms_->start();
        if (heartbeatIntervalMs > 0) {
            std::thread([state, msCopy, heartbeatIntervalMs, heartbeatTimeoutMs] {
                heartbeatLoop(state, msCopy, heartbeatIntervalMs, heartbeatTimeoutMs);
            }).detach();
        }
    }

    ~RpcProtocol() { close(); }

    // 默认 RPC 超时（毫秒），对齐 JS 修复版 15s；0 表示关闭。
    void setDefaultTimeoutMs(int64_t ms) { defaultTimeoutMs_.store(ms); }
    int64_t defaultTimeoutMs() const { return defaultTimeoutMs_.load(); }

    void registerMethod(const std::string& name, Handler h) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->handlers[name] = std::move(h);
    }

    void onEvent(std::function<void(uint8_t, Json)> cb) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->onEvent = std::move(cb);
    }

    void onError(std::function<void(const StreamError&)> cb) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->onError = std::move(cb);
    }

    // 发送 JSON 请求并阻塞等待响应；对端错误响应抛 RpcException(remote=true)。
    // timeoutMs：-1 用默认超时，0 关闭超时；超时抛 RpcException(code="ETIMEDOUT")，对齐 JS 修复版。
    Json sendRequest(const std::string& method, Json params = Json::null(), const std::string& to = "",
                     int64_t timeoutMs = -1) {
        ensureAuth();
        std::string id = newUUID();
        auto pending = std::make_shared<Pending>();
        {
            std::lock_guard<std::mutex> lock(state_->mu);
            state_->pending[id] = pending;
        }
        Json payload = Json::object();
        payload.set("method", Json::ofStr(method));
        payload.set("params", std::move(params));
        if (!to.empty()) payload.set("to", Json::ofStr(to));
        std::string body = payload.dump();
        if (!ms_->send(buildMessage(TYPE_REQUEST, CONTENT_JSON, id,
                                    reinterpret_cast<const uint8_t*>(body.data()), body.size()))) {
            removePending(id);
            throw RpcException("send failed");
        }
        return waitResult(id, pending, method, timeoutMs);
    }

    // 二进制请求：handler 实参为 [Bytes(data), ...args]；timeoutMs 语义同 sendRequest。
    Json sendRequestBinary(const std::string& method, const std::vector<uint8_t>& data,
                           const std::vector<Json>& args = {}, int64_t timeoutMs = -1) {
        ensureAuth();
        std::string id = newUUID();
        auto pending = std::make_shared<Pending>();
        {
            std::lock_guard<std::mutex> lock(state_->mu);
            state_->pending[id] = pending;
        }
        std::vector<uint8_t> body = encodeBinaryRequest(method, data, args);
        if (!ms_->send(buildMessage(TYPE_REQUEST, CONTENT_BINARY, id, body.data(), body.size()))) {
            removePending(id);
            throw RpcException("send failed");
        }
        return waitResult(id, pending, method, timeoutMs);
    }

    // 单向事件
    bool sendText(const std::string& text) {
        return ms_->send(buildMessage(TYPE_EVENT, CONTENT_TEXT, newUUID(),
                                      reinterpret_cast<const uint8_t*>(text.data()), text.size()));
    }

    bool sendJson(const Json& value) {
        std::string body = value.dump();
        return ms_->send(buildMessage(TYPE_EVENT, CONTENT_JSON, newUUID(),
                                      reinterpret_cast<const uint8_t*>(body.data()), body.size()));
    }

    bool sendBinary(const std::vector<uint8_t>& data) {
        return ms_->send(buildMessage(TYPE_EVENT, CONTENT_BINARY, newUUID(), data.data(), data.size()));
    }

    // 销毁：拒绝所有 pending 并关闭流。
    void close() {
        bool expected = false;
        if (!state_->closed.compare_exchange_strong(expected, true)) return;
        rejectAll(state_, "RPC Protocol destroyed");
        ms_->close();
    }

    // ---- v1.1: HELLO 握手 ----

    // ---- v1.1.3: 自动握手（P2） ----

    // 首个业务请求前自动 HELLO：仅客户端（requiredToken 为空且持有 token）生效；锁内只握手一次。
    void ensureAuth() {
        if (!state_->requiredToken.empty() || authToken_.empty()) return;
        if (state_->authenticated.load() || state_->authSent.load()) return;
        std::lock_guard<std::mutex> lock(state_->authMu);
        if (state_->authenticated.load() || state_->authSent.load()) return;
        hello();
        state_->authSent.store(true);
    }

    // 是否已完成（或被跳过）自动握手。
    bool authSent() const { return state_->authSent.load() || state_->authenticated.load(); }

    // 发起 HELLO 握手；ACK error → EAUTH，版本不符 → EVERSION。timeoutMs 语义同 sendRequest。
    Json hello(int64_t timeoutMs = -1) {
        std::string id = newUUID();
        auto pending = std::make_shared<Pending>();
        {
            std::lock_guard<std::mutex> lock(state_->mu);
            state_->pending[id] = pending;
        }
        Json payload = Json::object().set("version", Json::ofNum(PROTOCOL_VERSION));
        payload.set("caps", capsJson(state_->capabilities));
        payload.set("impl", Json::ofStr(state_->implName));
        if (!authToken_.empty()) payload.set("token", Json::ofStr(authToken_));
        std::string body = payload.dump();
        if (!ms_->send(buildMessage(TYPE_HELLO, CONTENT_JSON, id,
                                    reinterpret_cast<const uint8_t*>(body.data()), body.size()))) {
            removePending(id);
            throw RpcException("send failed");
        }
        return waitResult(id, pending, "hello", timeoutMs);
    }

    // ---- v1.1.2: 能力协商（PROTOCOL.md §8.4） ----
    std::vector<std::string> capabilities() const { return state_->capabilities; }
    std::string peerCapabilities() const { return state_->peerCapabilities; }
    std::string serverCapabilities() const { return state_->serverCapabilities; }
    std::string heartbeatDisabledReason() const { return state_->heartbeatReason; }

    void setCapabilities(const std::vector<std::string>& caps, const std::string& implName = "") {
        state_->capabilities = caps;
        if (!implName.empty()) state_->implName = implName;
    }

    static Json capsJson(const std::vector<std::string>& caps) {
        std::vector<Json> items;
        for (const auto& c : caps) items.push_back(Json::ofStr(c));
        return Json::ofArr(std::move(items));
    }

    static std::vector<std::string> readCaps(const Json& content) {
        std::vector<std::string> out;
        const Json* caps = content.find("caps");
        if (!caps || caps->type != Json::Type::Arr) return out;
        for (const auto& item : caps->arr) {
            if (item.type == Json::Type::Str) out.push_back(item.str);
        }
        return out;
    }

    static bool hasPing(const std::vector<std::string>& caps) {
        for (const auto& c : caps) if (c == "ping") return true;
        return false;
    }

private:
    struct Pending {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;
        Json value;
        std::unique_ptr<RpcException> err;
    };

    struct State {
        std::mutex mu;
        std::map<std::string, Handler> handlers;
        std::map<std::string, std::shared_ptr<Pending>> pending;
        std::function<void(uint8_t, Json)> onEvent;
        std::function<void(const StreamError&)> onError;
        // ---- v1.1: 心跳与 HELLO 握手（PROTOCOL.md §8.1/§8.2） ----
        std::atomic<bool> closed{false};
        std::atomic<bool> authenticated{true};
        std::atomic<int64_t> lastReceived{0};
        std::string requiredToken;
        // ---- v1.1.2: 能力协商（PROTOCOL.md §8.4） ----
        std::vector<std::string> capabilities{"ping", "binary", "event"};
        std::string implName{"cpp"};
        std::string peerCapabilities;   // 逗号分隔；空 = 旧实现（未声明）
        std::string serverCapabilities; // 客户端侧记录 HELLO_ACK 的 caps
        std::atomic<bool> heartbeatDisabled{false};
        std::string heartbeatReason;
        std::atomic<int> unauthorizedHits{0};
        // ---- v1.1.3: 首个业务请求前自动握手（P2） ----
        std::atomic<bool> authSent{false};
        std::mutex authMu;
    };

    std::shared_ptr<State> state_;
    std::atomic<int64_t> defaultTimeoutMs_{15000};
    std::string authToken_;
    std::shared_ptr<MessageStream> ms_; // 最后声明：析构时先停读线程

    void removePending(const std::string& id) {
        std::lock_guard<std::mutex> lock(state_->mu);
        state_->pending.erase(id);
    }

    Json waitResult(const std::string& id, std::shared_ptr<Pending> pending, const std::string& method,
                    int64_t timeoutMs) {
        int64_t effective = timeoutMs < 0 ? defaultTimeoutMs_.load() : timeoutMs;
        Json value;
        std::unique_ptr<RpcException> err;
        {
            std::unique_lock<std::mutex> lock(pending->mu);
            if (effective > 0) {
                if (!pending->cv.wait_for(lock, std::chrono::milliseconds(effective),
                                          [&] { return pending->done; })) {
                    // 超时：先移除 pending 再抛错，避免迟到响应写入已废弃的 pending
                    lock.unlock();
                    removePending(id);
                    throw RpcException("RPC timeout after " + std::to_string(effective) + "ms: " + method,
                                       "ETIMEDOUT");
                }
            } else {
                pending->cv.wait(lock, [&] { return pending->done; });
            }
            value = std::move(pending->value);
            err = std::move(pending->err);
        } // 先释放 pending 锁再取 state 锁，避免与 rejectAll 的锁序形成环
        removePending(id);
        if (err) throw std::move(*err);
        return value;
    }

    static void rejectAll(const std::shared_ptr<State>& state, const std::string& msg,
                          const std::string& code = "") {
        std::lock_guard<std::mutex> lock(state->mu);
        for (auto& [id, p] : state->pending) {
            std::lock_guard<std::mutex> pl(p->mu);
            p->err = std::make_unique<RpcException>(msg, code);
            p->done = true;
            p->cv.notify_all();
        }
        state->pending.clear();
    }

    // 心跳节拍：静默超过 timeout 以 EHEARTBEAT 结算全部 pending 并断链；否则按间隔发 PING。
    static void heartbeatLoop(const std::shared_ptr<State>& state, const std::shared_ptr<MessageStream>& ms,
                              int64_t intervalMs, int64_t timeoutMs) {
        for (;;) {
            std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
            if (state->closed.load()) return;
            if (state->heartbeatDisabled.load()) return;   // v1.1.2：能力协商停用心跳
            int64_t silent = nowMs() - state->lastReceived.load();
            if (timeoutMs > 0 && silent >= timeoutMs) {
                std::string msg = "heartbeat timeout: no data for " + std::to_string(silent) + "ms";
                rejectAll(state, msg, "EHEARTBEAT");
                {
                    std::lock_guard<std::mutex> lock(state->mu);
                    if (state->onError) state->onError(StreamError{msg, "EHEARTBEAT"});
                }
                bool expected = false;
                if (state->closed.compare_exchange_strong(expected, true)) ms->close();
                return;
            }
            if (silent >= intervalMs) {
                ms->send(buildMessage(TYPE_PING, CONTENT_TEXT, newUUID(),
                                      reinterpret_cast<const uint8_t*>(""), 0));
            }
        }
    }

    static void deliver(const std::shared_ptr<Pending>& p, Json value, std::unique_ptr<RpcException> err) {
        std::lock_guard<std::mutex> lock(p->mu);
        p->value = std::move(value);
        p->err = std::move(err);
        p->done = true;
        p->cv.notify_all();
    }

    static void handleMessage(const std::vector<uint8_t>& data, const std::shared_ptr<State>& state,
                              const std::shared_ptr<MessageStream>& ms) {
        if (data.size() < 18) return;
        uint8_t msgType = data[0];
        uint8_t contentType = data[1];
        if (msgType > 6) return;
        std::string id = bytesToUUID(data.data() + 2);
        const uint8_t* payload = data.data() + 18;
        size_t payloadLen = data.size() - 18;
        state->lastReceived.store(nowMs()); // 任何入站消息都刷新心跳基准

        Json content;
        try {
            content = deserialize(contentType, payload, payloadLen);
        } catch (...) {
            // 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版）
            if (msgType == TYPE_REQUEST) {
                sendErrorResponse(ms, id, "Invalid request payload");
            }
            return; // 解析失败：丢弃，不影响后续消息
        }

        if (msgType == TYPE_REQUEST) {
            if (!state->requiredToken.empty() && !state->authenticated.load()) {
                sendErrorResponse(ms, id, "Unauthorized");
                // v1.1.2：回错误但不断链（允许随后 HELLO 重试）；连续 5 次才断开
                if (state->unauthorizedHits.fetch_add(1) >= 4) {
                    bool expected = false;
                    if (state->closed.compare_exchange_strong(expected, true)) ms->close();
                }
                return;
            }
            std::vector<uint8_t> raw(payload, payload + payloadLen);
            std::thread([state, ms, id, contentType, content, raw]() mutable {
                handleRequest(id, contentType, content, raw, state, ms);
            }).detach();
        } else if (msgType == TYPE_RESPONSE) {
            std::shared_ptr<Pending> p;
            {
                std::lock_guard<std::mutex> lock(state->mu);
                auto it = state->pending.find(id);
                if (it != state->pending.end()) {
                    p = it->second;
                    state->pending.erase(it);
                }
            }
            if (p) {
                const Json* ev = content.find("error");
                if (content.type == Json::Type::Obj && ev && ev->isTruthy()) {
                    deliver(p, Json::null(), std::make_unique<RpcException>(*ev));
                } else {
                    deliver(p, std::move(content), nullptr);
                }
            }
        } else if (msgType == TYPE_EVENT) {
            if (!state->requiredToken.empty() && !state->authenticated.load()) return; // 未鉴权事件直接丢弃
            std::lock_guard<std::mutex> lock(state->mu);
            if (state->onEvent) state->onEvent(contentType, std::move(content));
        } else if (msgType == TYPE_PING) {
            ms->send(buildMessage(TYPE_PONG, CONTENT_TEXT, id,
                                  reinterpret_cast<const uint8_t*>(""), 0));
        } else if (msgType == TYPE_PONG) {
            // 仅刷新 lastReceived（上面已做）
        } else if (msgType == TYPE_HELLO) {
            handleHello(id, content, state, ms);
        } else if (msgType == TYPE_HELLO_ACK) {
            std::shared_ptr<Pending> p;
            {
                std::lock_guard<std::mutex> lock(state->mu);
                auto it = state->pending.find(id);
                if (it != state->pending.end()) {
                    p = it->second;
                    state->pending.erase(it);
                }
            }
            if (p) deliverHelloAck(p, content, state);
        }
    }

    // 服务端 HELLO 处理：版本/token 校验，失败回 error ACK 后断链。
    static void handleHello(const std::string& id, const Json& content, const std::shared_ptr<State>& state,
                            const std::shared_ptr<MessageStream>& ms) {
        Json ack = Json::object().set("version", Json::ofNum(PROTOCOL_VERSION));
        bool ok = false;
        if (content.type == Json::Type::Obj) {
            const Json* v = content.find("version");
            if (!v || v->type != Json::Type::Num || v->number != PROTOCOL_VERSION) {
                ack.set("error", Json::ofStr("Unsupported protocol version"));
            } else if (!state->requiredToken.empty()) {
                const Json* t = content.find("token");
                if (!t || t->type != Json::Type::Str || t->str != state->requiredToken) {
                    ack.set("error", Json::ofStr("Unauthorized"));
                } else {
                    state->authenticated.store(true);
                    ack.set("ok", Json::ofBool(true));
                    ok = true;
                }
            } else {
                state->authenticated.store(true);
                // v1.1.2：能力协商
                auto peer = readCaps(content);
                std::string peerJoined;
                for (const auto& c : peer) peerJoined += (peerJoined.empty() ? "" : ",") + c;
                state->peerCapabilities = peerJoined;
                if (!peer.empty() && !hasPing(peer)) {
                    const Json* impl = content.find("impl");
                    std::string implName = (impl && impl->type == Json::Type::Str) ? impl->str : "unknown";
                    state->heartbeatDisabled.store(true);
                    state->heartbeatReason = "peer " + implName + " declares no 'ping' capability";
                }
                ack.set("ok", Json::ofBool(true));
                ack.set("caps", capsJson(state->capabilities));
                ack.set("impl", Json::ofStr(state->implName));
                ok = true;
            }
        } else {
            ack.set("error", Json::ofStr("Invalid hello payload"));
        }
        std::string body = ack.dump();
        ms->send(buildMessage(TYPE_HELLO_ACK, CONTENT_JSON, id,
                              reinterpret_cast<const uint8_t*>(body.data()), body.size()));
        if (!ok) {
            // v1.1.2：先让 ack 刷到对端再断链
            std::thread([state, ms]() {
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
                bool expected = false;
                if (state->closed.compare_exchange_strong(expected, true)) ms->close();
            }).detach();
        }
    }

    // 客户端 HELLO_ACK 解析：error → EAUTH，版本不符 → EVERSION。
    static void deliverHelloAck(const std::shared_ptr<Pending>& p, const Json& content,
                                const std::shared_ptr<State>& state) {
        if (content.type != Json::Type::Obj) {
            deliver(p, Json::null(), std::make_unique<RpcException>("Invalid hello ack payload", "EAUTH"));
            return;
        }
        const Json* ev = content.find("error");
        if (ev && ev->isTruthy()) {
            std::string msg = ev->type == Json::Type::Str ? ev->str : ev->dump();
            deliver(p, Json::null(), std::make_unique<RpcException>(msg, "EAUTH"));
            return;
        }
        const Json* v = content.find("version");
        if (!v || v->type != Json::Type::Num || v->number != PROTOCOL_VERSION) {
            deliver(p, Json::null(), std::make_unique<RpcException>("Unsupported protocol version", "EVERSION"));
            return;
        }
        // v1.1.2：记录服务端能力；服务端不支持 PING 则本端停用心跳
        auto caps = readCaps(content);
        std::string joined;
        for (const auto& c : caps) joined += (joined.empty() ? "" : ",") + c;
        state->serverCapabilities = joined;
        if (!caps.empty() && !hasPing(caps)) {
            state->heartbeatDisabled.store(true);
            state->heartbeatReason = "server declares no 'ping' capability";
        }
        deliver(p, content, nullptr);
    }

    static void sendErrorResponse(const std::shared_ptr<MessageStream>& ms, const std::string& id,
                                  const std::string& msg) {
        std::string body = Json::object().set("error", Json::ofStr(msg)).dump();
        ms->send(buildMessage(TYPE_RESPONSE, CONTENT_JSON, id,
                              reinterpret_cast<const uint8_t*>(body.data()), body.size()));
    }

    static void handleRequest(const std::string& id, uint8_t contentType, const Json& content,
                              const std::vector<uint8_t>& raw, const std::shared_ptr<State>& state,
                              const std::shared_ptr<MessageStream>& ms) {
        std::string method;
        std::vector<Json> args;
        if (contentType == CONTENT_BINARY) {
            if (!decodeBinaryRequest(raw, method, args)) {
                sendErrorResponse(ms, id, "Invalid request payload: bad binary request");
                return;
            }
        } else {
            if (content.type != Json::Type::Obj) {
                sendErrorResponse(ms, id, "Invalid request payload: not an object");
                return;
            }
            const Json* m = content.find("method");
            if (!m || m->type != Json::Type::Str) {
                sendErrorResponse(ms, id, "Invalid request payload: method missing");
                return;
            }
            method = m->str;
            const Json* params = content.find("params");
            args = expandParams(params ? *params : Json::null());
        }
        if (method.empty()) {
            sendErrorResponse(ms, id, "Invalid request payload: method missing");
            return;
        }

        Handler handler;
        {
            std::lock_guard<std::mutex> lock(state->mu);
            auto it = state->handlers.find(method);
            if (it != state->handlers.end()) handler = it->second;
        }

        uint8_t respType;
        std::vector<uint8_t> respBody;
        if (!handler) {
            respType = CONTENT_JSON;
            std::string body = Json::object().set("error", Json::ofStr("Method not found: " + method)).dump();
            respBody.assign(body.begin(), body.end());
        } else {
            try {
                Json result = handler(args);
                serializeResponse(result, respType, respBody);
            } catch (const std::exception& e) {
                respType = CONTENT_JSON;
                std::string body = Json::object().set("error", Json::ofStr(e.what())).dump();
                respBody.assign(body.begin(), body.end());
            }
        }
        ms->send(buildMessage(TYPE_RESPONSE, respType, id, respBody.data(), respBody.size()));
    }

    // ---- 序列化辅助 ----

    static Json deserialize(uint8_t contentType, const uint8_t* payload, size_t len) {
        std::string text(reinterpret_cast<const char*>(payload), len);
        switch (contentType) {
            case CONTENT_TEXT: return Json::ofStr(std::move(text));
            case CONTENT_JSON: return Json::parse(text);
            case CONTENT_BINARY: return Json::ofBytes(std::vector<uint8_t>(payload, payload + len));
            default: throw std::runtime_error("unknown content type");
        }
    }

    static void serializeResponse(const Json& v, uint8_t& type, std::vector<uint8_t>& body) {
        switch (v.type) {
            case Json::Type::Null:
                type = CONTENT_JSON;
                body = {'n', 'u', 'l', 'l'};
                break;
            case Json::Type::Str:
                type = CONTENT_TEXT;
                body.assign(v.str.begin(), v.str.end());
                break;
            case Json::Type::Bytes:
                type = CONTENT_BINARY;
                body = v.bytes;
                break;
            default: {
                type = CONTENT_JSON;
                std::string s = v.dump();
                body.assign(s.begin(), s.end());
                break;
            }
        }
    }

    static std::vector<Json> expandParams(const Json& params) {
        if (params.type == Json::Type::Null) return {};
        if (params.type == Json::Type::Arr) return params.arr;
        return {params};
    }

    static std::vector<uint8_t> buildMessage(uint8_t msgType, uint8_t contentType, const std::string& id,
                                             const uint8_t* payload, size_t len) {
        std::vector<uint8_t> msg(18 + len);
        msg[0] = msgType;
        msg[1] = contentType;
        auto idBytes = uuidToBytes(id);
        std::memcpy(msg.data() + 2, idBytes.data(), 16);
        std::memcpy(msg.data() + 18, payload, len);
        return msg;
    }

    static void writeBe32(std::vector<uint8_t>& out, uint32_t v) {
        out.push_back(static_cast<uint8_t>(v >> 24));
        out.push_back(static_cast<uint8_t>(v >> 16));
        out.push_back(static_cast<uint8_t>(v >> 8));
        out.push_back(static_cast<uint8_t>(v));
    }

    static uint32_t readBe32(const uint8_t* p) {
        return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
               (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
    }

    // [methodLen u32BE][method][paramsLen u32BE][paramsJSON][binary]
    static std::vector<uint8_t> encodeBinaryRequest(const std::string& method, const std::vector<uint8_t>& data,
                                                    const std::vector<Json>& args) {
        std::string paramsJson = Json::ofArr(args).dump();
        std::vector<uint8_t> out;
        writeBe32(out, static_cast<uint32_t>(method.size()));
        out.insert(out.end(), method.begin(), method.end());
        writeBe32(out, static_cast<uint32_t>(paramsJson.size()));
        out.insert(out.end(), paramsJson.begin(), paramsJson.end());
        out.insert(out.end(), data.begin(), data.end());
        return out;
    }

    static bool decodeBinaryRequest(const std::vector<uint8_t>& raw, std::string& method, std::vector<Json>& args) {
        if (raw.size() < 4) return false;
        uint32_t methodLen = readBe32(raw.data());
        if (raw.size() < 4 + methodLen + 4) return false;
        method.assign(reinterpret_cast<const char*>(raw.data() + 4), methodLen);
        size_t off = 4 + methodLen;
        uint32_t paramsLen = readBe32(raw.data() + off);
        if (raw.size() < off + 4 + paramsLen) return false;
        try {
            Json params = Json::parse(std::string(reinterpret_cast<const char*>(raw.data() + off + 4), paramsLen));
            args.clear();
            args.push_back(Json::ofBytes(std::vector<uint8_t>(raw.begin() + off + 4 + paramsLen, raw.end())));
            if (params.type == Json::Type::Arr) {
                for (auto& p : params.arr) args.push_back(std::move(p));
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    // ---- UUID（RFC 4122 大端字节序）----

    static std::string newUUID() {
        static std::mutex mu;
        static std::mt19937_64 rng(std::random_device{}());
        std::lock_guard<std::mutex> lock(mu);
        uint64_t a = rng(), b = rng();
        uint8_t bytes[16];
        std::memcpy(bytes, &a, 8);
        std::memcpy(bytes + 8, &b, 8);
        bytes[6] = (bytes[6] & 0x0f) | 0x40;
        bytes[8] = (bytes[8] & 0x3f) | 0x80;
        return bytesToUUID(bytes);
    }

    static std::vector<uint8_t> uuidToBytes(const std::string& id) {
        std::string hex;
        for (char c : id) if (c != '-') hex += c;
        std::vector<uint8_t> out(16);
        for (int i = 0; i < 16; i++) {
            out[i] = static_cast<uint8_t>(std::stoul(hex.substr(i * 2, 2), nullptr, 16));
        }
        return out;
    }

    static std::string bytesToUUID(const uint8_t* b) {
        char buf[37];
        snprintf(buf, sizeof(buf),
                 "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                 b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                 b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
        return std::string(buf);
    }
};

} // namespace ipc