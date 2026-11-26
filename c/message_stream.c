#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "message_stream.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct { unsigned char* data; size_t head, len, cap; } Buf;

static void buf_append(Buf* b, const unsigned char* data, size_t n) {
    if (b->head + b->len + n > b->cap) {
        while (b->head + b->len + n > b->cap) b->cap = b->cap ? b->cap * 2 : 4096;
        b->data = (unsigned char*)realloc(b->data, b->cap);
    }
    memcpy(b->data + b->head + b->len, data, n);
    b->len += n;
}

/* 消费只前移游标，避免 O(n^2) 全量前移；达阈值时摊还 compact */
static void buf_consume(Buf* b, size_t n) {
    b->head += n;
    b->len -= n;
    if (b->head >= 1024 * 1024 || b->head >= b->len) {
        memmove(b->data, b->data + b->head, b->len);
        b->head = 0;
    }
}

static void buf_clear(Buf* b) {
    b->head = 0;
    b->len = 0;
}

static unsigned read_le32(const unsigned char* p) {
    return (unsigned)p[0] | ((unsigned)p[1] << 8) | ((unsigned)p[2] << 16) | ((unsigned)p[3] << 24);
}

static void write_le32(unsigned char* p, unsigned v) {
    p[0] = (unsigned char)v;
    p[1] = (unsigned char)(v >> 8);
    p[2] = (unsigned char)(v >> 16);
    p[3] = (unsigned char)(v >> 24);
}

static unsigned crc_table[256];
static pthread_once_t crc_once = PTHREAD_ONCE_INIT;

static void crc_init(void) {
    for (unsigned i = 0; i < 256; i++) {
        unsigned c = i;
        for (int k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc_table[i] = c;
    }
}

unsigned ipc_crc32(const unsigned char* data, size_t len) {
    pthread_once(&crc_once, crc_init);
    unsigned crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) crc = crc_table[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

struct MessageStream {
    IpcIo io;
    int enableCRC;
    size_t maxBufferSize;

    void (*on_message)(void*, const unsigned char*, size_t);
    void* on_message_user;
    void (*on_error)(void*, const char*, const char*);
    void* on_error_user;
    void (*on_sync)(void*);
    void* on_sync_user;

    pthread_mutex_t send_mu;
    int started;
    pthread_t thread;

    /* 跨线程状态：门控/关闭/重同步计时 */
    pthread_mutex_t state_mu;
    pthread_cond_t sync_cv;     /* 门控唤醒 + 计时提前唤醒 */
    pthread_cond_t watchers_cv; /* 计时线程归零通知 */
    int closed;
    int io_closed;
    int protocolMode;
    int sync_timeout_ms;
    unsigned long long sync_generation;
    int sync_watchers;

    /* 以下缓冲状态仅读线程访问 */
    Buf buf;
    long expectedLen;
};

typedef struct { MessageStream* ms; unsigned long long gen; int timeout; } SyncWatchArg;

MessageStream* ms_create(IpcIo io, int enableCRC, size_t maxBufferSize) {
    MessageStream* ms = (MessageStream*)calloc(1, sizeof(MessageStream));
    ms->io = io;
    ms->enableCRC = enableCRC;
    ms->maxBufferSize = maxBufferSize ? maxBufferSize : 256 * 1024 * 1024;
    ms->expectedLen = -1;
    ms->sync_timeout_ms = 3000;
    pthread_mutex_init(&ms->send_mu, NULL);
    pthread_mutex_init(&ms->state_mu, NULL);
    pthread_cond_init(&ms->sync_cv, NULL);
    pthread_cond_init(&ms->watchers_cv, NULL);
    return ms;
}

void ms_on_message(MessageStream* ms, void (*cb)(void*, const unsigned char*, size_t), void* user) {
    ms->on_message = cb;
    ms->on_message_user = user;
}

void ms_on_error(MessageStream* ms, void (*cb)(void*, const char*, const char*), void* user) {
    ms->on_error = cb;
    ms->on_error_user = user;
}

void ms_on_sync_complete(MessageStream* ms, void (*cb)(void*), void* user) {
    ms->on_sync = cb;
    ms->on_sync_user = user;
}

static void emit_error(MessageStream* ms, const char* msg, const char* code) {
    if (ms->on_error) ms->on_error(ms->on_error_user, msg, code);
}

/* 退出协议模式：作废旧计时并唤醒门控中的发送方 */
static void exit_protocol_mode(MessageStream* ms) {
    pthread_mutex_lock(&ms->state_mu);
    ms->protocolMode = 0;
    ms->sync_generation++;
    pthread_cond_broadcast(&ms->sync_cv);
    pthread_mutex_unlock(&ms->state_mu);
}

/* 重同步计时：超时仍未收到 RESET_CONFIRM 则以 ERESYNC_TIMEOUT 断链。
 * 用 cond_timedwait 以便 close/退出协议模式时提前唤醒，避免 destroy 阻塞满超时。 */
static void* sync_watch(void* arg) {
    SyncWatchArg* a = (SyncWatchArg*)arg;
    MessageStream* ms = a->ms;
    pthread_mutex_lock(&ms->state_mu);
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += a->timeout / 1000;
    ts.tv_nsec += (long)(a->timeout % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
    while (ms->protocolMode && !ms->closed && ms->sync_generation == a->gen) {
        if (pthread_cond_timedwait(&ms->sync_cv, &ms->state_mu, &ts) == ETIMEDOUT) break;
    }
    int fire = ms->protocolMode && !ms->closed && ms->sync_generation == a->gen;
    if (fire) {
        ms->closed = 1;
        ms->io_closed = 1;
        pthread_cond_broadcast(&ms->sync_cv);
    }
    ms->sync_watchers--;
    if (ms->sync_watchers == 0) pthread_cond_broadcast(&ms->watchers_cv);
    pthread_mutex_unlock(&ms->state_mu);
    if (fire) {
        emit_error(ms, "resync handshake timeout", "ERESYNC_TIMEOUT");
        if (ms->io.close) ms->io.close(ms->io.ctx);
    }
    free(a);
    return NULL;
}

static void ms_reset(MessageStream* ms) {
    buf_clear(&ms->buf);
    ms->expectedLen = -1;
    pthread_mutex_lock(&ms->state_mu);
    ms->protocolMode = 1;
    unsigned long long gen = ++ms->sync_generation;
    int timeout = ms->sync_timeout_ms;
    if (timeout > 0) ms->sync_watchers++;
    pthread_mutex_unlock(&ms->state_mu);
    if (timeout > 0) {
        SyncWatchArg* a = (SyncWatchArg*)malloc(sizeof(SyncWatchArg));
        a->ms = ms; a->gen = gen; a->timeout = timeout;
        pthread_t t;
        if (pthread_create(&t, NULL, sync_watch, a) != 0) {
            pthread_mutex_lock(&ms->state_mu);
            ms->sync_watchers--;
            if (ms->sync_watchers == 0) pthread_cond_broadcast(&ms->watchers_cv);
            pthread_mutex_unlock(&ms->state_mu);
            free(a);
        } else {
            pthread_detach(t);
        }
    }
}

/* 底层帧发送（旁路门控）：错误包/协议消息与已放行后的业务帧共用 */
static int send_frame(MessageStream* ms, const unsigned char* data, size_t len) {
    size_t headerSize = ms->enableCRC ? 8 : 4;
    size_t total = headerSize + len + (ms->enableCRC ? 4 : 0);
    unsigned char* frame = (unsigned char*)malloc(total);
    write_le32(frame, (unsigned)len);
    if (ms->enableCRC) write_le32(frame + 4, ipc_crc32(frame, 4));
    memcpy(frame + headerSize, data, len);
    if (ms->enableCRC) write_le32(frame + headerSize + len, ipc_crc32(data, len));
    pthread_mutex_lock(&ms->send_mu);
    int ok = ms->io.write(ms->io.ctx, frame, total);
    pthread_mutex_unlock(&ms->send_mu);
    free(frame);
    return ok;
}

int ms_send(MessageStream* ms, const unsigned char* data, size_t len) {
    if (len > ms->maxBufferSize) return 0;
    pthread_mutex_lock(&ms->state_mu);
    while (ms->protocolMode && !ms->closed) pthread_cond_wait(&ms->sync_cv, &ms->state_mu);
    int closed = ms->closed;
    pthread_mutex_unlock(&ms->state_mu);
    if (closed) return 0;
    return send_frame(ms, data, len);
}

int ms_send_error(MessageStream* ms, unsigned char code) {
    unsigned char pkt[5] = {0xEE, 0xEE, 0xEE, 0xEE, code};
    return send_frame(ms, pkt, 5);
}

int ms_send_protocol(MessageStream* ms, const char* data, size_t len) {
    size_t total = 8 + len;
    unsigned char* frame = (unsigned char*)malloc(total);
    write_le32(frame, 0xFFFFFFFFu);
    write_le32(frame + 4, (unsigned)len);
    memcpy(frame + 8, data, len);
    pthread_mutex_lock(&ms->send_mu);
    int ok = ms->io.write(ms->io.ctx, frame, total);
    pthread_mutex_unlock(&ms->send_mu);
    free(frame);
    return ok;
}

static void process_buffer(MessageStream* ms) {
    for (;;) {
        if (ms->buf.len > ms->maxBufferSize) {
            emit_error(ms, "buffer overflow", "BUFFER_OVERFLOW");
            ms_send_error(ms, IPC_ERR_BUFFER_OVERFLOW);
            ms_reset(ms);
            return;
        }
        if (ms->protocolMode) {
            /* 扫描协议头，而不是整段丢弃（避免误丢 RESET_CONFIRM） */
            size_t window = ms->buf.len < 65536 ? ms->buf.len : 65536;
            long idx = -1;
            for (size_t i = 0; i + 4 <= window; i++) {
                if (read_le32(ms->buf.data + ms->buf.head + i) == 0xFFFFFFFFu) { idx = (long)i; break; }
            }
            if (idx >= 0) {
                buf_consume(&ms->buf, (size_t)idx);
                if (ms->buf.len < 8) return; /* 等待长度字段 */
                unsigned length = read_le32(ms->buf.data + ms->buf.head + 4);
                if (length > 50) {
                    buf_consume(&ms->buf, 8); /* 伪头：跳过继续扫描 */
                    continue;
                }
                if (ms->buf.len < 8 + length) return; /* 协议消息未收全 */
                char text[64];
                memcpy(text, ms->buf.data + ms->buf.head + 8, length);
                text[length] = '\0';
                buf_consume(&ms->buf, 8 + length);
                /* trim */
                char* t = text;
                while (*t == ' ' || *t == '\t' || *t == '\n' || *t == '\r') t++;
                size_t n = strlen(t);
                while (n > 0 && (t[n - 1] == ' ' || t[n - 1] == '\t' || t[n - 1] == '\n' || t[n - 1] == '\r')) n--;
                t[n] = '\0';
                if (strcmp(t, "RESET_CONFIRM") == 0) {
                    buf_clear(&ms->buf);
                    exit_protocol_mode(ms);
                    if (ms->on_sync) ms->on_sync(ms->on_sync_user);
                }
                continue;
            }
            /* 窗口内无协议头：丢弃已扫描部分，保留尾部 3 字节（可能半截头） */
            size_t keep = ms->buf.len < 3 ? ms->buf.len : 3;
            buf_consume(&ms->buf, ms->buf.len - keep);
            return;
        }

        size_t headerSize = ms->enableCRC ? 8 : 4;
        if (ms->expectedLen < 0) {
            if (ms->buf.len < headerSize) return;
            unsigned length = read_le32(ms->buf.data + ms->buf.head);
            if (ms->enableCRC) {
                unsigned expected = read_le32(ms->buf.data + ms->buf.head + 4);
                if (ipc_crc32(ms->buf.data + ms->buf.head, 4) != expected) {
                    ms_send_error(ms, IPC_ERR_LENGTH_CRC_FAILED);
                    emit_error(ms, "length header CRC check failed", "LENGTH_CRC_FAILED");
                    ms_reset(ms);
                    return;
                }
            }
            /* 长度头强制校验：0 或超限视为坏帧（CRC 合法也可能是伪造/损坏的长度） */
            if (length == 0 || (size_t)length > ms->maxBufferSize) {
                unsigned char code = (size_t)length > ms->maxBufferSize
                                         ? IPC_ERR_BUFFER_OVERFLOW : IPC_ERR_LENGTH_CRC_FAILED;
                ms_send_error(ms, code);
                char msg[64];
                snprintf(msg, sizeof(msg), "invalid frame length %u", length);
                emit_error(ms, msg, "BAD_LENGTH");
                ms_reset(ms);
                return;
            }
            ms->expectedLen = (long)length;
            buf_consume(&ms->buf, headerSize);
        }

        size_t totalSize = (size_t)ms->expectedLen + (ms->enableCRC ? 4 : 0);
        if (ms->buf.len < totalSize) return;
        unsigned char* data = (unsigned char*)malloc((size_t)ms->expectedLen);
        memcpy(data, ms->buf.data + ms->buf.head, (size_t)ms->expectedLen);
        if (ms->enableCRC) {
            unsigned expected = read_le32(ms->buf.data + ms->buf.head + ms->expectedLen);
            if (ipc_crc32(data, (size_t)ms->expectedLen) != expected) {
                free(data);
                ms_send_error(ms, IPC_ERR_MESSAGE_CRC_FAILED);
                emit_error(ms, "message CRC check failed", "MESSAGE_CRC_FAILED");
                ms_reset(ms);
                return;
            }
        }
        size_t dataLen = (size_t)ms->expectedLen;
        buf_consume(&ms->buf, totalSize);
        ms->expectedLen = -1;
        /* 错误包可能 reset 清空缓冲区，故先消费再分发 */
        if (dataLen == 5 && data[0] == 0xEE && data[1] == 0xEE && data[2] == 0xEE && data[3] == 0xEE) {
            free(data);
            emit_error(ms, "peer reported error packet", "ERESYNC");
            ms_reset(ms);
            ms_send_protocol(ms, "RESET_CONFIRM", 13);
            exit_protocol_mode(ms);
        } else {
            if (ms->on_message) ms->on_message(ms->on_message_user, data, dataLen);
            free(data);
        }
        if (ms->protocolMode) return;
    }
}

static void* read_loop(void* arg) {
    MessageStream* ms = (MessageStream*)arg;
    unsigned char tmp[65536];
    for (;;) {
        int n = ms->io.read(ms->io.ctx, tmp, (int)sizeof(tmp));
        if (n > 0) {
            buf_append(&ms->buf, tmp, (size_t)n);
            process_buffer(ms);
        }
        if (n <= 0) {
            if (!ms->closed) {
                if (n == 0) emit_error(ms, "stream ended", "ECLOSED");
                else emit_error(ms, "stream read error", "ESOCKET");
            }
            return NULL;
        }
    }
}

void ms_start(MessageStream* ms) {
    if (ms->started) return;
    ms->started = 1;
    pthread_create(&ms->thread, NULL, read_loop, ms);
}

void ms_set_sync_timeout(MessageStream* ms, int timeoutMs) {
    ms->sync_timeout_ms = timeoutMs;
}

int ms_is_closed(MessageStream* ms) {
    pthread_mutex_lock(&ms->state_mu);
    int c = ms->closed;
    pthread_mutex_unlock(&ms->state_mu);
    return c;
}

void ms_close(MessageStream* ms) {
    pthread_mutex_lock(&ms->state_mu);
    int do_close = !ms->io_closed;
    ms->io_closed = 1;
    ms->closed = 1;
    pthread_cond_broadcast(&ms->sync_cv);
    pthread_mutex_unlock(&ms->state_mu);
    if (do_close && ms->io.close) ms->io.close(ms->io.ctx);
}

void ms_destroy(MessageStream* ms) {
    ms_close(ms);
    if (ms->started) pthread_join(ms->thread, NULL);
    /* 等重同步计时线程退出，避免 use-after-free */
    pthread_mutex_lock(&ms->state_mu);
    while (ms->sync_watchers > 0) pthread_cond_wait(&ms->watchers_cv, &ms->state_mu);
    pthread_mutex_unlock(&ms->state_mu);
    free(ms->buf.data);
    pthread_mutex_destroy(&ms->send_mu);
    pthread_mutex_destroy(&ms->state_mu);
    pthread_cond_destroy(&ms->sync_cv);
    pthread_cond_destroy(&ms->watchers_cv);
    free(ms);
}