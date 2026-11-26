// MessageStream 帧层：长度头分帧 + 可选 CRC32 + 错误包与重同步。
// 回调必须在 start() 之前设置；RpcProtocol 会自动处理。
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace ipc {

constexpr uint8_t ERR_LENGTH_CRC_FAILED = 1;
constexpr uint8_t ERR_MESSAGE_CRC_FAILED = 2;
constexpr uint8_t ERR_BUFFER_OVERFLOW = 3;

// 带错误码的流错误（对齐 JS 修复版 ipcError 的 code 字段）。
struct StreamError {
    std::string message;
    std::string code;
};

// 传输抽象：任意双向字节流（TCP/管道等）。read 返回字节数，0=EOF，-1=错误。
class ByteStream {
public:
    virtual ~ByteStream() = default;
    virtual int read(uint8_t* buf, int max) = 0;
    virtual bool write(const uint8_t* data, size_t len) = 0;
    virtual void close() = 0;
};

// CRC32（反射多项式 0xEDB88320，与 zlib 一致）
inline uint32_t crc32(const uint8_t* data, size_t len) {
    // C++11 魔法静态：线程安全的一次性初始化
    static const std::vector<uint32_t> table = [] {
        std::vector<uint32_t> t(256);
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            t[i] = c;
        }
        return t;
    }();
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) crc = table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

inline uint32_t readLe32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

inline void writeLe32(uint8_t* p, uint32_t v) {
    p[0] = static_cast<uint8_t>(v);
    p[1] = static_cast<uint8_t>(v >> 8);
    p[2] = static_cast<uint8_t>(v >> 16);
    p[3] = static_cast<uint8_t>(v >> 24);
}

class MessageStream : public std::enable_shared_from_this<MessageStream> {
public:
    std::function<void(const std::vector<uint8_t>&)> onMessage;
    std::function<void(const StreamError&)> onError;
    std::function<void()> onSyncComplete;

    // syncTimeoutMs：重同步握手超时（PROTOCOL.md §4.5），<=0 关闭。
    MessageStream(std::shared_ptr<ByteStream> io, bool enableCRC = true,
                  size_t maxBufferSize = 256 * 1024 * 1024, int64_t syncTimeoutMs = 3000)
        : io_(std::move(io)), enableCRC_(enableCRC), maxBufferSize_(maxBufferSize),
          syncTimeoutMs_(syncTimeoutMs) {}

    ~MessageStream() {
        close();
        if (readThread_.joinable()) readThread_.join();
    }

    // 启动读循环（幂等）。必须先设置回调。
    void start() {
        bool expected = false;
        if (!started_.compare_exchange_strong(expected, true)) return;
        readThread_ = std::thread([this] { readLoop(); });
    }

    void close() {
        closed_.store(true);
        syncGeneration_.fetch_add(1);
        {
            std::lock_guard<std::mutex> lock(gateMu_);
        }
        gateCv_.notify_all(); // 唤醒门控中的发送方，使其以关闭失败
        io_->close();
    }

    bool isClosed() const { return closed_.load(); }

    // 发送一个完整帧（并发安全）。协议模式期间门控等待重同步完成（对齐 JS _syncGate）。
    bool send(const uint8_t* data, size_t len) {
        if (len > maxBufferSize_) return false;
        {
            std::unique_lock<std::mutex> lock(gateMu_);
            gateCv_.wait(lock, [this] { return !protocolMode_.load() || closed_.load(); });
        }
        if (closed_.load()) return false;
        return sendFrame(data, len);
    }

    bool send(const std::vector<uint8_t>& data) { return send(data.data(), data.size()); }

    // 错误包（按正常帧封装的 EEEEEEEE+code）；不受重同步门控。
    bool sendError(uint8_t code) {
        uint8_t pkt[5] = {0xEE, 0xEE, 0xEE, 0xEE, code};
        return sendFrame(pkt, 5);
    }

    // 裸协议帧（FFFFFFFF+长度+数据，无 CRC）
    bool sendProtocolMessage(const std::string& data) {
        std::vector<uint8_t> frame(8 + data.size());
        writeLe32(frame.data(), 0xFFFFFFFFu);
        writeLe32(frame.data() + 4, static_cast<uint32_t>(data.size()));
        std::memcpy(frame.data() + 8, data.data(), data.size());
        std::lock_guard<std::mutex> lock(sendMu_);
        return io_->write(frame.data(), frame.size());
    }

private:
    std::shared_ptr<ByteStream> io_;
    bool enableCRC_;
    size_t maxBufferSize_;
    int64_t syncTimeoutMs_;
    std::mutex sendMu_;
    std::atomic<bool> started_{false};
    std::atomic<bool> closed_{false};
    std::thread readThread_;

    // 重同步门控与计时状态（跨线程）
    std::mutex gateMu_;
    std::condition_variable gateCv_;
    std::atomic<bool> protocolMode_{false};
    std::atomic<uint64_t> syncGeneration_{0}; // 世代号：退出协议模式时递增作废旧计时

    // 以下缓冲状态仅读线程访问
    std::vector<uint8_t> buf_;
    size_t head_ = 0; // 逻辑读游标：消费只前移游标，避免 O(n^2) 头部删除
    long expectedLen_ = -1;

    void emitError(const std::string& msg, const std::string& code) {
        if (onError) onError(StreamError{msg, code});
    }

    // 直接写帧，不受重同步门控（错误包旁路用）。
    bool sendFrame(const uint8_t* data, size_t len) {
        size_t headerSize = enableCRC_ ? 8 : 4;
        std::vector<uint8_t> frame(headerSize + len + (enableCRC_ ? 4 : 0));
        writeLe32(frame.data(), static_cast<uint32_t>(len));
        if (enableCRC_) writeLe32(frame.data() + 4, crc32(frame.data(), 4));
        std::memcpy(frame.data() + headerSize, data, len);
        if (enableCRC_) writeLe32(frame.data() + headerSize + len, crc32(data, len));
        std::lock_guard<std::mutex> lock(sendMu_);
        return io_->write(frame.data(), frame.size());
    }

    size_t bufLen() const { return buf_.size() - head_; }
    const uint8_t* bufData() const { return buf_.data() + head_; }

    void consume(size_t n) {
        head_ += n;
        if (head_ >= 1024 * 1024 || head_ >= bufLen()) {
            buf_.erase(buf_.begin(), buf_.begin() + head_);
            head_ = 0;
        }
    }

    void clearBuf() {
        buf_.clear();
        head_ = 0;
    }

    // 在 64KB 窗口内扫描协议头，返回相对偏移；未找到返回 -1
    ptrdiff_t findProtocolHeader() const {
        size_t window = bufLen() < 64 * 1024 ? bufLen() : 64 * 1024;
        if (window < 4) return -1;
        const uint8_t* d = bufData();
        for (size_t i = 0; i + 4 <= window; i++) {
            if (readLe32(d + i) == 0xFFFFFFFFu) return static_cast<ptrdiff_t>(i);
        }
        return -1;
    }

    void reset() {
        clearBuf();
        expectedLen_ = -1;
        protocolMode_.store(true);
        uint64_t gen = syncGeneration_.fetch_add(1) + 1;
        if (syncTimeoutMs_ > 0) {
            std::weak_ptr<MessageStream> weak = weak_from_this();
            std::thread([weak, gen] {
                auto self = weak.lock();
                if (self) self->syncWatch(gen);
            }).detach();
        }
    }

    // 退出协议模式：作废旧计时并唤醒门控中的发送方。
    void exitProtocolMode() {
        protocolMode_.store(false);
        syncGeneration_.fetch_add(1);
        {
            std::lock_guard<std::mutex> lock(gateMu_);
        }
        gateCv_.notify_all();
    }

    // 重同步计时：超时仍未收到 RESET_CONFIRM 则以 ERESYNC_TIMEOUT 断链。
    void syncWatch(uint64_t gen) {
        std::this_thread::sleep_for(std::chrono::milliseconds(syncTimeoutMs_));
        {
            std::lock_guard<std::mutex> lock(gateMu_);
            if (gen != syncGeneration_.load() || !protocolMode_.load() || closed_.load()) return;
            protocolMode_.store(false);
            syncGeneration_.fetch_add(1);
        }
        gateCv_.notify_all();
        emitError("resync handshake timeout", "ERESYNC_TIMEOUT");
        close();
    }

    void readLoop() {
        std::vector<uint8_t> tmp(65536);
        for (;;) {
            int n = io_->read(tmp.data(), static_cast<int>(tmp.size()));
            if (n > 0) {
                buf_.insert(buf_.end(), tmp.begin(), tmp.begin() + n);
                processBuffer();
            }
            if (n <= 0) {
                if (!closed_.load()) {
                    if (n == 0) emitError("stream ended", "ECLOSED");
                    else emitError("stream read error", "ESOCKET");
                }
                return;
            }
        }
    }

    void processBuffer() {
        for (;;) {
            if (bufLen() > maxBufferSize_) {
                emitError("buffer overflow", "BUFFER_OVERFLOW");
                sendError(ERR_BUFFER_OVERFLOW);
                reset();
                return;
            }
            if (protocolMode_.load()) {
                // 扫描协议头，而不是整段丢弃（避免误丢 RESET_CONFIRM）
                ptrdiff_t idx = findProtocolHeader();
                if (idx >= 0) {
                    consume(static_cast<size_t>(idx));
                    if (bufLen() < 8) return; // 等待长度字段
                    uint32_t length = readLe32(bufData() + 4);
                    if (length > 50) {
                        consume(8); // 伪头：跳过继续扫描
                        continue;
                    }
                    if (bufLen() < 8 + length) return; // 协议消息未收全
                    std::string text(reinterpret_cast<const char*>(bufData() + 8), length);
                    consume(8 + length);
                    // trim
                    size_t b = text.find_first_not_of(" \t\n\r");
                    size_t e = text.find_last_not_of(" \t\n\r");
                    std::string trimmed = (b == std::string::npos) ? "" : text.substr(b, e - b + 1);
                    if (trimmed == "RESET_CONFIRM") {
                        exitProtocolMode();
                        clearBuf();
                        if (onSyncComplete) onSyncComplete();
                    }
                    continue;
                }
                // 窗口内无协议头：丢弃已扫描部分，保留尾部 3 字节（可能半截头）
                size_t keep = bufLen() < 3 ? bufLen() : 3;
                consume(bufLen() - keep);
                return;
            }

            size_t headerSize = enableCRC_ ? 8 : 4;
            if (expectedLen_ < 0) {
                if (bufLen() < headerSize) return;
                uint32_t length = readLe32(bufData());
                if (enableCRC_) {
                    uint32_t expected = readLe32(bufData() + 4);
                    if (crc32(bufData(), 4) != expected) {
                        sendError(ERR_LENGTH_CRC_FAILED);
                        emitError("length header CRC check failed", "LENGTH_CRC_FAILED");
                        reset();
                        return;
                    }
                }
                // 长度头强制校验：0 或超限视为坏帧（CRC 合法也可能是伪造/损坏的长度）
                if (length == 0 || static_cast<uint64_t>(length) > maxBufferSize_) {
                    uint8_t code = static_cast<uint64_t>(length) > maxBufferSize_
                                       ? ERR_BUFFER_OVERFLOW : ERR_LENGTH_CRC_FAILED;
                    sendError(code);
                    emitError("invalid frame length " + std::to_string(length), "BAD_LENGTH");
                    reset();
                    return;
                }
                expectedLen_ = static_cast<long>(length);
                consume(headerSize);
            }

            size_t totalSize = static_cast<size_t>(expectedLen_) + (enableCRC_ ? 4 : 0);
            if (bufLen() < totalSize) return;
            std::vector<uint8_t> data(bufData(), bufData() + expectedLen_);
            if (enableCRC_) {
                uint32_t expected = readLe32(bufData() + expectedLen_);
                if (crc32(data.data(), data.size()) != expected) {
                    sendError(ERR_MESSAGE_CRC_FAILED);
                    emitError("message CRC check failed", "MESSAGE_CRC_FAILED");
                    reset();
                    return;
                }
            }
            consume(totalSize);
            expectedLen_ = -1;
            // 错误包可能 reset 清空缓冲区，故先消费再分发
            if (data.size() == 5 && data[0] == 0xEE && data[1] == 0xEE && data[2] == 0xEE && data[3] == 0xEE) {
                emitError("peer reported error packet", "ERESYNC");
                reset();
                sendProtocolMessage("RESET_CONFIRM");
                exitProtocolMode();
            } else if (onMessage) {
                onMessage(data);
            }
            if (protocolMode_.load()) return;
        }
    }
};

} // namespace ipc