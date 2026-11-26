#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "rpc.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static char* dup_cstr(const char* s) {
    size_t n = strlen(s) + 1;
    char* p = (char*)malloc(n);
    memcpy(p, s, n);
    return p;
}

#define TYPE_REQUEST  0
#define TYPE_RESPONSE 1
#define TYPE_EVENT    2
#define TYPE_PING     3
#define TYPE_PONG     4
#define TYPE_HELLO    5
#define TYPE_HELLO_ACK 6

#define PROTOCOL_VERSION 1

#define CONTENT_TEXT   0
#define CONTENT_JSON   1
#define CONTENT_BINARY 2

static long long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

typedef struct Pending {
    char id[37];
    pthread_mutex_t mu;
    pthread_cond_t cv;
    int done;
    RpcResult result;
    struct Pending* next;
} Pending;

typedef struct HandlerEntry {
    char* name;
    IpcHandler fn;
    void* user;
    struct HandlerEntry* next;
} HandlerEntry;

struct RpcProtocol {
    MessageStream* ms;
    pthread_mutex_t mu; /* 保护 handlers/pending/回调/active */
    HandlerEntry* handlers;
    Pending* pending;
    void (*event_cb)(void*, int, JsonValue*);
    void* event_user;
    void (*error_cb)(void*, const char*, const char*);
    void* error_user;
    int active; /* 进行中的请求处理线程数 */
    pthread_cond_t active_cv;
    int closed;
    int default_timeout_ms; /* 默认 RPC 超时，对齐 JS 修复版 15s；0 关闭 */
    /* v1.1：心跳与 HELLO 握手 */
    int heartbeat_interval_ms;
    int heartbeat_timeout_ms;
    char* auth_token;
    char* required_token;
    int authenticated;
    long long last_received_ms;
    pthread_cond_t hb_cv;
    int hb_started;
    pthread_t hb_thread;
    /* v1.1.2：能力协商（PROTOCOL.md §8.4） */
    char* caps;        /* 本端声明，逗号分隔 */
    char* impl_name;
    char* peer_caps;   /* 对端 HELLO 声明的能力；NULL = 旧实现 */
    char* server_caps; /* 客户端侧 HELLO_ACK 声明的能力 */
    int hb_disabled;
    char hb_reason[160];
    int unauth_hits;
    /* v1.1.3：首个业务请求前自动握手（P2） */
    int auth_sent;
    pthread_mutex_t auth_mu;
};

typedef struct {
    RpcProtocol* rpc;
    char id[37];
    int contentType;
    unsigned char* raw;
    size_t rawLen;
    JsonValue* content;
} RequestJob;

/* ---- UUID（RFC 4122 大端字节序；xorshift PRNG 保证唯一性）---- */

static pthread_mutex_t uuid_mu = PTHREAD_MUTEX_INITIALIZER;
static unsigned long long uuid_state = 0;

static unsigned long long uuid_rand(void) {
    if (uuid_state == 0) {
        uuid_state = (unsigned long long)time(NULL) * 2654435761ull ^
                     (unsigned long long)(uintptr_t)&uuid_state;
        if (uuid_state == 0) uuid_state = 1;
    }
    uuid_state ^= uuid_state << 13;
    uuid_state ^= uuid_state >> 7;
    uuid_state ^= uuid_state << 17;
    return uuid_state;
}

static void new_uuid(char out[37]) {
    pthread_mutex_lock(&uuid_mu);
    unsigned char b[16];
    unsigned long long a = uuid_rand(), c = uuid_rand();
    memcpy(b, &a, 8);
    memcpy(b + 8, &c, 8);
    pthread_mutex_unlock(&uuid_mu);
    b[6] = (unsigned char)((b[6] & 0x0f) | 0x40);
    b[8] = (unsigned char)((b[8] & 0x3f) | 0x80);
    snprintf(out, 37, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
}

static void uuid_to_bytes(const char* id, unsigned char out[16]) {
    char hex[33];
    int j = 0;
    for (int i = 0; id[i] && j < 32; i++) if (id[i] != '-') hex[j++] = id[i];
    hex[j] = '\0';
    for (int i = 0; i < 16; i++) {
        char pair[3] = {hex[i * 2], hex[i * 2 + 1], '\0'};
        out[i] = (unsigned char)strtoul(pair, NULL, 16);
    }
}

static void bytes_to_uuid(const unsigned char* b, char out[37]) {
    snprintf(out, 37, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
}

/* ---- 消息构建/解析辅助 ---- */

static unsigned char* build_message(int msgType, int contentType, const char* id,
                                    const unsigned char* payload, size_t len, size_t* outLen) {
    unsigned char* msg = (unsigned char*)malloc(18 + len);
    msg[0] = (unsigned char)msgType;
    msg[1] = (unsigned char)contentType;
    uuid_to_bytes(id, msg + 2);
    memcpy(msg + 18, payload, len);
    *outLen = 18 + len;
    return msg;
}

static JsonValue* deserialize(int contentType, const unsigned char* payload, size_t len) {
    char* text = (char*)malloc(len + 1);
    memcpy(text, payload, len);
    text[len] = '\0';
    JsonValue* v = NULL;
    switch (contentType) {
        case CONTENT_TEXT: v = json_str(text); break;
        case CONTENT_JSON: v = json_parse(text); break;
        case CONTENT_BINARY: v = json_bytes(payload, len); break;
        default: break;
    }
    free(text);
    return v;
}

static void write_be32(unsigned char* p, unsigned v) {
    p[0] = (unsigned char)(v >> 24);
    p[1] = (unsigned char)(v >> 16);
    p[2] = (unsigned char)(v >> 8);
    p[3] = (unsigned char)v;
}

static unsigned read_be32(const unsigned char* p) {
    return ((unsigned)p[0] << 24) | ((unsigned)p[1] << 16) | ((unsigned)p[2] << 8) | (unsigned)p[3];
}

/* [methodLen u32BE][method][paramsLen u32BE][paramsJSON][binary] */
static unsigned char* encode_binary_request(const char* method, const unsigned char* data, size_t dataLen,
                                            JsonValue* argsArr, size_t* outLen) {
    char* paramsJson = argsArr ? json_dump(argsArr) : NULL;
    if (!paramsJson) { paramsJson = (char*)malloc(3); strcpy(paramsJson, "[]"); }
    size_t methodLen = strlen(method), paramsLen = strlen(paramsJson);
    size_t total = 4 + methodLen + 4 + paramsLen + dataLen;
    unsigned char* out = (unsigned char*)malloc(total);
    write_be32(out, (unsigned)methodLen);
    memcpy(out + 4, method, methodLen);
    write_be32(out + 4 + methodLen, (unsigned)paramsLen);
    memcpy(out + 8 + methodLen, paramsJson, paramsLen);
    memcpy(out + 8 + methodLen + paramsLen, data, dataLen);
    free(paramsJson);
    *outLen = total;
    return out;
}

/* ---- pending 管理 ---- */

static Pending* pending_take(RpcProtocol* rpc, const char* id) {
    pthread_mutex_lock(&rpc->mu);
    Pending** pp = &rpc->pending;
    while (*pp) {
        if (strcmp((*pp)->id, id) == 0) {
            Pending* p = *pp;
            *pp = p->next;
            pthread_mutex_unlock(&rpc->mu);
            return p;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&rpc->mu);
    return NULL;
}

static void pending_remove(RpcProtocol* rpc, const char* id) {
    Pending* p = pending_take(rpc, id);
    if (p) {
        pthread_mutex_destroy(&p->mu);
        pthread_cond_destroy(&p->cv);
        free(p);
    }
}

static void deliver(Pending* p, JsonValue* value, JsonValue* error, int remote, const char* code) {
    pthread_mutex_lock(&p->mu);
    p->result.value = value;
    p->result.error = error;
    p->result.remote = remote;
    p->result.code = code;
    p->done = 1;
    pthread_cond_signal(&p->cv);
    pthread_mutex_unlock(&p->mu);
}

static void reject_all(RpcProtocol* rpc, const char* msg, const char* code) {
    pthread_mutex_lock(&rpc->mu);
    Pending* p = rpc->pending;
    rpc->pending = NULL;
    pthread_mutex_unlock(&rpc->mu);
    while (p) {
        Pending* next = p->next;
        deliver(p, NULL, json_str(msg), 0, code);
        p = next;
    }
}

/* ---- 请求处理 ---- */

static void send_response(RpcProtocol* rpc, const char* id, JsonValue* result, char* errMsg) {
    int respType;
    unsigned char* body = NULL;
    size_t bodyLen = 0;
    if (errMsg) {
        JsonValue* e = json_obj();
        json_obj_set(e, "error", json_str(errMsg));
        char* s = json_dump(e);
        json_free(e);
        respType = CONTENT_JSON;
        bodyLen = strlen(s);
        body = (unsigned char*)s; /* 直接转移所有权 */
    } else if (!result || result->type == JSON_NULL) {
        respType = CONTENT_JSON;
        body = (unsigned char*)malloc(4);
        memcpy(body, "null", 4);
        bodyLen = 4;
    } else if (result->type == JSON_STR) {
        respType = CONTENT_TEXT;
        bodyLen = strlen(result->u.str);
        body = (unsigned char*)malloc(bodyLen ? bodyLen : 1);
        memcpy(body, result->u.str, bodyLen);
    } else if (result->type == JSON_BYTES) {
        respType = CONTENT_BINARY;
        bodyLen = result->u.bytes.len;
        body = (unsigned char*)malloc(bodyLen ? bodyLen : 1);
        memcpy(body, result->u.bytes.data, bodyLen);
    } else {
        char* s = json_dump(result);
        respType = CONTENT_JSON;
        bodyLen = strlen(s);
        body = (unsigned char*)s;
    }
    size_t msgLen;
    unsigned char* msg = build_message(TYPE_RESPONSE, respType, id, body, bodyLen, &msgLen);
    ms_send(rpc->ms, msg, msgLen);
    free(msg);
    free(body);
}

static void* request_thread(void* arg) {
    RequestJob* job = (RequestJob*)arg;
    RpcProtocol* rpc = job->rpc;

    char* method = NULL;
    JsonValue** args = NULL;
    size_t nargs = 0;
    JsonValue* ownedBytes = NULL;

    if (job->contentType == CONTENT_BINARY) {
        const unsigned char* raw = job->raw;
        size_t n = job->rawLen;
        if (n >= 4) {
            unsigned methodLen = read_be32(raw);
            if (n >= 4 + methodLen + 4) {
                method = (char*)malloc(methodLen + 1);
                memcpy(method, raw + 4, methodLen);
                method[methodLen] = '\0';
                size_t off = 4 + methodLen;
                unsigned paramsLen = read_be32(raw + off);
                if (n >= off + 4 + paramsLen) {
                    char* paramsText = (char*)malloc(paramsLen + 1);
                    memcpy(paramsText, raw + off + 4, paramsLen);
                    paramsText[paramsLen] = '\0';
                    JsonValue* params = json_parse(paramsText);
                    free(paramsText);
                    size_t extra = (params && params->type == JSON_ARR) ? params->u.arr.len : 0;
                    args = (JsonValue**)malloc((extra + 1) * sizeof(JsonValue*));
                    ownedBytes = json_bytes(raw + off + 4 + paramsLen, n - off - 4 - paramsLen);
                    args[0] = ownedBytes;
                    nargs = 1;
                    for (size_t i = 0; i < extra; i++) args[nargs++] = params->u.arr.items[i];
                    /* 元素所有权转移给 args，仅释放容器 */
                    params->u.arr.len = 0;
                    json_free(params);
                }
            }
        }
    } else if (job->content && job->content->type == JSON_OBJ) {
        JsonValue* m = json_get(job->content, "method");
        if (m && m->type == JSON_STR) {
            method = dup_cstr(m->u.str);
            JsonValue* params = json_get(job->content, "params");
            if (params && params->type == JSON_ARR) {
                nargs = params->u.arr.len;
                args = (JsonValue**)malloc((nargs ? nargs : 1) * sizeof(JsonValue*));
                for (size_t i = 0; i < nargs; i++) args[i] = params->u.arr.items[i];
            } else if (params && params->type != JSON_NULL) {
                args = (JsonValue**)malloc(sizeof(JsonValue*));
                args[0] = params;
                nargs = 1;
            }
        }
    }

    if (!method || !method[0]) {
        /* 畸形请求（非对象/无 method/二进制解码失败）：回错误响应而非静默丢弃 */
        send_response(rpc, job->id, NULL, "Invalid request payload");
    } else {
        IpcHandler fn = NULL;
        void* user = NULL;
        pthread_mutex_lock(&rpc->mu);
        for (HandlerEntry* e = rpc->handlers; e; e = e->next) {
            if (strcmp(e->name, method) == 0) { fn = e->fn; user = e->user; break; }
        }
        pthread_mutex_unlock(&rpc->mu);

        if (fn) {
            char* errMsg = NULL;
            JsonValue* result = fn(user, args, nargs, &errMsg);
            send_response(rpc, job->id, result, errMsg);
            json_free(result);
            free(errMsg);
        } else {
            char* msg = (char*)malloc(strlen(method) + 32);
            sprintf(msg, "Method not found: %s", method);
            send_response(rpc, job->id, NULL, msg);
            free(msg);
        }
    }

    free(method);
    /* 二进制路径的附加参数已从 params 容器接管所有权，需逐个释放 */
    if (job->contentType == CONTENT_BINARY) {
        for (size_t i = 1; i < nargs; i++) json_free(args[i]);
    }
    free(args);
    json_free(ownedBytes);
    json_free(job->content);
    free(job->raw);

    pthread_mutex_lock(&rpc->mu);
    rpc->active--;
    pthread_cond_broadcast(&rpc->active_cv);
    pthread_mutex_unlock(&rpc->mu);
    free(job);
    return NULL;
}

/* ---- v1.1：HELLO 握手与心跳 ---- */

/* v1.1.2：把 HELLO/HELLO_ACK 的 caps 数组拼成逗号串；未声明返回 NULL（旧实现） */
static char* caps_csv(const JsonValue* content) {
    if (!content || content->type != JSON_OBJ) return NULL;
    JsonValue* caps = json_get(content, "caps");
    if (!caps || caps->type != JSON_ARR) return NULL;
    size_t cap = 64 + caps->u.arr.len * 24;
    char* out = (char*)calloc(cap, 1);
    size_t used = 0;
    for (size_t i = 0; i < caps->u.arr.len; i++) {
        JsonValue* it = caps->u.arr.items[i];
        if (!it || it->type != JSON_STR) continue;
        size_t n = strlen(it->u.str);
        if (used + n + 2 >= cap) break;
        if (used) out[used++] = ',';
        memcpy(out + used, it->u.str, n);
        used += n;
    }
    out[used] = '\0';
    return out;
}

static int caps_has_ping(const char* csv) {
    if (!csv) return 0;
    const char* p = csv;
    while (p && *p) {
        const char* comma = strchr(p, ',');
        size_t n = comma ? (size_t)(comma - p) : strlen(p);
        if (n == 4 && memcmp(p, "ping", 4) == 0) return 1;
        p = comma ? comma + 1 : NULL;
    }
    return 0;
}

/* 逗号串 → JSON 数组（用于 ACK 回传） */
static JsonValue* caps_to_json(const char* csv) {
    JsonValue* arr = json_arr();
    if (csv && csv[0]) {
        const char* p = csv;
        while (p && *p) {
            const char* comma = strchr(p, ',');
            size_t n = comma ? (size_t)(comma - p) : strlen(p);
            char* item = (char*)calloc(n + 1, 1);
            memcpy(item, p, n);
            json_arr_push(arr, json_str(item));
            free(item);
            p = comma ? comma + 1 : NULL;
        }
    }
    return arr;
}

/* 服务端 HELLO 处理：版本/token 校验，失败回 error ACK 后断链。 */
static void handle_hello(RpcProtocol* rpc, const char* id, JsonValue* content) {
    JsonValue* ack = json_obj();
    json_obj_set(ack, "version", json_num(PROTOCOL_VERSION));
    int ok = 0;
    if (content && content->type == JSON_OBJ) {
        JsonValue* v = json_get(content, "version");
        if (!v || v->type != JSON_NUM || v->u.number != PROTOCOL_VERSION) {
            json_obj_set(ack, "error", json_str("Unsupported protocol version"));
        } else {
            pthread_mutex_lock(&rpc->mu);
            const char* req = rpc->required_token;
            int need = req && req[0];
            JsonValue* t = json_get(content, "token");
            int tokOk = !need || (t && t->type == JSON_STR && strcmp(t->u.str, req) == 0);
            if (tokOk) {
                rpc->authenticated = 1;
                /* v1.1.2：记录对端能力；对端声明了但不含 ping → 停用心跳 */
                free(rpc->peer_caps);
                rpc->peer_caps = caps_csv(content);
                if (rpc->peer_caps && !caps_has_ping(rpc->peer_caps)) {
                    JsonValue* impl = json_get(content, "impl");
                    const char* name = (impl && impl->type == JSON_STR) ? impl->u.str : "unknown";
                    rpc->hb_disabled = 1;
                    snprintf(rpc->hb_reason, sizeof(rpc->hb_reason),
                             "peer %s declares no 'ping' capability", name);
                }
                ok = 1;
            }
            pthread_mutex_unlock(&rpc->mu);
            if (ok) {
                json_obj_set(ack, "ok", json_bool(1));
                json_obj_set(ack, "caps", caps_to_json(rpc->caps));
                json_obj_set(ack, "impl", json_str(rpc->impl_name ? rpc->impl_name : "c"));
            } else {
                json_obj_set(ack, "error", json_str("Unauthorized"));
            }
        }
    } else {
        json_obj_set(ack, "error", json_str("Invalid hello payload"));
    }
    char* body = json_dump(ack);
    size_t n;
    unsigned char* m = build_message(TYPE_HELLO_ACK, CONTENT_JSON, id, (unsigned char*)body, strlen(body), &n);
    ms_send(rpc->ms, m, n);
    free(m);
    free(body);
    json_free(ack);
    if (!ok) rpc_close(rpc);   /* C 版 ms_send 为同步写，ack 已进内核缓冲，可直接断链 */
}

/* 客户端 HELLO_ACK 解析：error → EAUTH，版本不符 → EVERSION。 */
static void handle_hello_ack(RpcProtocol* rpc, const char* id, JsonValue* content) {
    Pending* p = pending_take(rpc, id);
    if (!p) return;
    if (!content || content->type != JSON_OBJ) {
        deliver(p, NULL, json_str("Invalid hello ack payload"), 0, "EAUTH");
        return;
    }
    JsonValue* ev = json_get(content, "error");
    if (ev && json_truthy(ev)) {
        char* s = ev->type == JSON_STR ? dup_cstr(ev->u.str) : json_dump(ev);
        deliver(p, NULL, json_str(s), 0, "EAUTH");
        free(s);
        return;
    }
    JsonValue* v = json_get(content, "version");
    if (!v || v->type != JSON_NUM || v->u.number != PROTOCOL_VERSION) {
        deliver(p, NULL, json_str("Unsupported protocol version"), 0, "EVERSION");
        return;
    }
    char* s = json_dump(content);
    JsonValue* copy = json_parse(s);
    free(s);
    /* v1.1.2：记录服务端能力；服务端不支持 PING 则本端停用心跳 */
    {
        char* caps = caps_csv(content);
        free(rpc->server_caps);
        rpc->server_caps = caps;
        if (caps && !caps_has_ping(caps)) {
            pthread_mutex_lock(&rpc->mu);
            rpc->hb_disabled = 1;
            snprintf(rpc->hb_reason, sizeof(rpc->hb_reason), "server declares no 'ping' capability");
            pthread_cond_broadcast(&rpc->hb_cv);
            pthread_mutex_unlock(&rpc->mu);
        }
    }
    deliver(p, copy ? copy : json_null(), NULL, 0, NULL);
}

/* 心跳节拍：静默超过 timeout 以 EHEARTBEAT 结算全部 pending 并断链；否则按间隔发 PING。 */
static void* heartbeat_thread(void* arg) {
    RpcProtocol* rpc = (RpcProtocol*)arg;
    for (;;) {
        pthread_mutex_lock(&rpc->mu);
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += rpc->heartbeat_interval_ms / 1000;
        ts.tv_nsec += (long)(rpc->heartbeat_interval_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
        while (!rpc->closed && pthread_cond_timedwait(&rpc->hb_cv, &rpc->mu, &ts) != ETIMEDOUT) {}
        int closed = rpc->closed;
        int disabled = rpc->hb_disabled;
        long long last = rpc->last_received_ms;
        int timeout = rpc->heartbeat_timeout_ms;
        int interval = rpc->heartbeat_interval_ms;
        pthread_mutex_unlock(&rpc->mu);
        if (closed) return NULL;
        if (disabled) return NULL;   /* v1.1.2：能力协商停用心跳 */
        long long silent = now_ms() - last;
        if (timeout > 0 && silent >= timeout) {
            char msg[128];
            snprintf(msg, sizeof(msg), "heartbeat timeout: no data for %lldms", silent);
            reject_all(rpc, msg, "EHEARTBEAT");
            pthread_mutex_lock(&rpc->mu);
            void (*cb)(void*, const char*, const char*) = rpc->error_cb;
            void* cu = rpc->error_user;
            pthread_mutex_unlock(&rpc->mu);
            if (cb) cb(cu, msg, "EHEARTBEAT");
            rpc_close(rpc);
            return NULL;
        }
        if (silent >= interval) {
            char id[37];
            new_uuid(id);
            size_t n;
            unsigned char* m = build_message(TYPE_PING, CONTENT_TEXT, id, (const unsigned char*)"", 0, &n);
            ms_send(rpc->ms, m, n);
            free(m);
        }
    }
}

/* ---- 流回调 ---- */

static void on_stream_message(void* user, const unsigned char* data, size_t len) {
    RpcProtocol* rpc = (RpcProtocol*)user;
    if (len < 18) return;
    int msgType = data[0], contentType = data[1];
    char id[37];
    bytes_to_uuid(data + 2, id);
    pthread_mutex_lock(&rpc->mu);
    rpc->last_received_ms = now_ms(); /* 任何入站消息都刷新心跳基准 */
    pthread_mutex_unlock(&rpc->mu);
    JsonValue* content = deserialize(contentType, data + 18, len - 18);

    if (msgType == TYPE_PING) {
        size_t n;
        unsigned char* m = build_message(TYPE_PONG, CONTENT_TEXT, id, (const unsigned char*)"", 0, &n);
        ms_send(rpc->ms, m, n);
        free(m);
        json_free(content);
        return;
    }
    if (msgType == TYPE_PONG) {
        json_free(content); /* 仅刷新 lastReceived（上面已做） */
        return;
    }
    if (msgType == TYPE_HELLO) {
        handle_hello(rpc, id, content);
        json_free(content);
        return;
    }
    if (msgType == TYPE_HELLO_ACK) {
        handle_hello_ack(rpc, id, content);
        json_free(content);
        return;
    }

    if (!content) {
        /* 畸形 REQUEST 尽量回错误响应，避免调用方永久等待（对齐 JS 修复版） */
        if (msgType == TYPE_REQUEST) {
            send_response(rpc, id, NULL, "Invalid request payload");
        }
        return;
    }

    if (msgType == TYPE_REQUEST) {
        pthread_mutex_lock(&rpc->mu);
        int authed = rpc->authenticated;
        int needs_auth = rpc->required_token && rpc->required_token[0];
        pthread_mutex_unlock(&rpc->mu);
        if (needs_auth && !authed) {
            send_response(rpc, id, NULL, "Unauthorized");
            /* v1.1.2：回错误但不断链（允许随后 HELLO 重试）；连续 5 次才断开 */
            pthread_mutex_lock(&rpc->mu);
            int hits = ++rpc->unauth_hits;
            pthread_mutex_unlock(&rpc->mu);
            if (hits >= 5) rpc_close(rpc);
            json_free(content);
            return;
        }
        RequestJob* job = (RequestJob*)calloc(1, sizeof(RequestJob));
        job->rpc = rpc;
        strcpy(job->id, id);
        job->contentType = contentType;
        job->rawLen = len - 18;
        job->raw = (unsigned char*)malloc(job->rawLen ? job->rawLen : 1);
        memcpy(job->raw, data + 18, job->rawLen);
        job->content = content;
        pthread_mutex_lock(&rpc->mu);
        rpc->active++;
        pthread_mutex_unlock(&rpc->mu);
        pthread_t t;
        if (pthread_create(&t, NULL, request_thread, job) != 0) {
            pthread_mutex_lock(&rpc->mu);
            rpc->active--;
            pthread_mutex_unlock(&rpc->mu);
            json_free(content);
            free(job->raw);
            free(job);
            return;
        }
        pthread_detach(t);
    } else if (msgType == TYPE_RESPONSE) {
        Pending* p = pending_take(rpc, id);
        if (p) {
            JsonValue* ev = content->type == JSON_OBJ ? json_get(content, "error") : NULL;
            if (ev && json_truthy(ev)) {
                /* 复制原始 error 值交付（content 随后释放） */
                char* s = json_dump(ev);
                JsonValue* copy = json_parse(s);
                free(s);
                deliver(p, NULL, copy ? copy : json_null(), 1, NULL);
            } else {
                deliver(p, content, NULL, 0, NULL);
                content = NULL; /* 所有权转移 */
            }
            /* p 由等待方 wait_pending 释放，避免交付后立即释放造成竞态 */
        }
        json_free(content);
    } else if (msgType == TYPE_EVENT) {
        pthread_mutex_lock(&rpc->mu);
        int authed = rpc->authenticated;
        int needs_auth = rpc->required_token && rpc->required_token[0];
        void (*cb)(void*, int, JsonValue*) = rpc->event_cb;
        void* cbUser = rpc->event_user;
        pthread_mutex_unlock(&rpc->mu);
        if ((!needs_auth || authed) && cb) cb(cbUser, contentType, content);
        json_free(content);
    } else {
        json_free(content);
    }
}

static void on_stream_error(void* user, const char* msg, const char* code) {
    RpcProtocol* rpc = (RpcProtocol*)user;
    char buf[512];
    snprintf(buf, sizeof(buf), "Request failed: %s", msg);
    reject_all(rpc, buf, code);
    pthread_mutex_lock(&rpc->mu);
    void (*cb)(void*, const char*, const char*) = rpc->error_cb;
    void* cbUser = rpc->error_user;
    pthread_mutex_unlock(&rpc->mu);
    if (cb) cb(cbUser, msg, code);
}

/* ---- 公共 API ---- */

static Pending* pending_new(RpcProtocol* rpc);
static RpcResult wait_pending(RpcProtocol* rpc, Pending* p, int timeoutMs, const char* method);

RpcProtocol* rpc_create_options(MessageStream* ms, int heartbeatIntervalMs, int heartbeatTimeoutMs,
                                const char* authToken, const char* requiredToken) {
    RpcProtocol* rpc = (RpcProtocol*)calloc(1, sizeof(RpcProtocol));
    rpc->ms = ms;
    rpc->default_timeout_ms = 15000;
    rpc->heartbeat_interval_ms = heartbeatIntervalMs;
    rpc->heartbeat_timeout_ms = heartbeatTimeoutMs;
    /* v1.1.2：未显式传 token 时回退环境变量 IPC_TOKEN（Node 主进程注入） */
    const char* envToken = getenv("IPC_TOKEN");
    const char* token = (authToken && authToken[0]) ? authToken : ((envToken && envToken[0]) ? envToken : NULL);
    rpc->auth_token = token ? dup_cstr(token) : NULL;
    rpc->required_token = requiredToken ? dup_cstr(requiredToken) : NULL;
    rpc->caps = dup_cstr("ping,binary,event");
    rpc->impl_name = dup_cstr("c");
    /* v1.1.3：区分服务端校验位与客户端握手完成，否则客户端会把自己当作已鉴权而跳过 HELLO */
    rpc->authenticated = !(requiredToken && requiredToken[0]) && !(token && token[0]);
    rpc->last_received_ms = now_ms();
    pthread_mutex_init(&rpc->mu, NULL);
    pthread_mutex_init(&rpc->auth_mu, NULL);
    pthread_cond_init(&rpc->active_cv, NULL);
    pthread_cond_init(&rpc->hb_cv, NULL);
    ms_on_message(ms, on_stream_message, rpc);
    ms_on_error(ms, on_stream_error, rpc);
    ms_start(ms);
    if (heartbeatIntervalMs > 0) {
        rpc->hb_started = 1;
        pthread_create(&rpc->hb_thread, NULL, heartbeat_thread, rpc);
    }
    return rpc;
}

RpcProtocol* rpc_create(MessageStream* ms) {
    return rpc_create_options(ms, 5000, 15000, NULL, NULL);
}

RpcResult rpc_hello(RpcProtocol* rpc, int timeoutMs) {
    if (timeoutMs < 0) timeoutMs = rpc->default_timeout_ms;
    Pending* p = pending_new(rpc);
    JsonValue* payload = json_obj();
    json_obj_set(payload, "version", json_num(PROTOCOL_VERSION));
    json_obj_set(payload, "caps", caps_to_json(rpc->caps));
    json_obj_set(payload, "impl", json_str(rpc->impl_name ? rpc->impl_name : "c"));
    if (rpc->auth_token && rpc->auth_token[0])
        json_obj_set(payload, "token", json_str(rpc->auth_token));
    char* body = json_dump(payload);
    json_free(payload);
    size_t n;
    unsigned char* m = build_message(TYPE_HELLO, CONTENT_JSON, p->id, (unsigned char*)body, strlen(body), &n);
    int ok = ms_send(rpc->ms, m, n);
    free(m);
    free(body);
    if (!ok) {
        pending_remove(rpc, p->id);
        RpcResult r = {NULL, json_str("send failed"), 0, NULL};
        return r;
    }
    return wait_pending(rpc, p, timeoutMs, "hello");
}

void rpc_set_default_timeout(RpcProtocol* rpc, int timeoutMs) {
    rpc->default_timeout_ms = timeoutMs;
}

/* ---- v1.1.2 能力协商访问器 ---- */
const char* rpc_capabilities(RpcProtocol* rpc) { return rpc->caps; }
const char* rpc_peer_capabilities(RpcProtocol* rpc) { return rpc->peer_caps; }
const char* rpc_server_capabilities(RpcProtocol* rpc) { return rpc->server_caps; }
const char* rpc_heartbeat_disabled_reason(RpcProtocol* rpc) {
    return rpc->hb_disabled ? rpc->hb_reason : NULL;
}

void rpc_set_capabilities(RpcProtocol* rpc, const char* capsCsv, const char* implName) {
    if (capsCsv) {
        free(rpc->caps);
        rpc->caps = dup_cstr(capsCsv);
    }
    if (implName) {
        free(rpc->impl_name);
        rpc->impl_name = dup_cstr(implName);
    }
}

void rpc_register(RpcProtocol* rpc, const char* name, IpcHandler h, void* user) {
    HandlerEntry* e = (HandlerEntry*)calloc(1, sizeof(HandlerEntry));
    e->name = dup_cstr(name);
    e->fn = h;
    e->user = user;
    pthread_mutex_lock(&rpc->mu);
    e->next = rpc->handlers;
    rpc->handlers = e;
    pthread_mutex_unlock(&rpc->mu);
}

void rpc_on_event(RpcProtocol* rpc, void (*cb)(void*, int, JsonValue*), void* user) {
    pthread_mutex_lock(&rpc->mu);
    rpc->event_cb = cb;
    rpc->event_user = user;
    pthread_mutex_unlock(&rpc->mu);
}

void rpc_on_error(RpcProtocol* rpc, void (*cb)(void*, const char*, const char*), void* user) {
    pthread_mutex_lock(&rpc->mu);
    rpc->error_cb = cb;
    rpc->error_user = user;
    pthread_mutex_unlock(&rpc->mu);
}

static void pending_free(Pending* p) {
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->cv);
    free(p);
}

static RpcResult wait_pending(RpcProtocol* rpc, Pending* p, int timeoutMs, const char* method) {
    pthread_mutex_lock(&p->mu);
    if (timeoutMs > 0) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeoutMs / 1000;
        ts.tv_nsec += (long)(timeoutMs % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }
        while (!p->done && pthread_cond_timedwait(&p->cv, &p->mu, &ts) != ETIMEDOUT) {}
    } else {
        while (!p->done) pthread_cond_wait(&p->cv, &p->mu);
    }
    int done = p->done;
    RpcResult r = p->result;
    pthread_mutex_unlock(&p->mu);
    if (done) {
        /* deliver 方已不再触碰 p，此处安全释放 */
        pending_free(p);
        return r;
    }
    /* 超时：先尝试从 pending 链表摘除；若响应线程已抢先取走，
     * 则等其 deliver 完成后释放迟到结果，避免泄漏/双重释放 */
    if (pending_take(rpc, p->id) == p) {
        pending_free(p);
    } else {
        pthread_mutex_lock(&p->mu);
        while (!p->done) pthread_cond_wait(&p->cv, &p->mu);
        RpcResult late = p->result;
        pthread_mutex_unlock(&p->mu);
        json_free(late.value);
        json_free(late.error);
        pending_free(p);
    }
    char msg[256];
    snprintf(msg, sizeof(msg), "RPC timeout after %dms: %s", timeoutMs, method);
    RpcResult timedOut = {NULL, json_str(msg), 0, "ETIMEDOUT"};
    return timedOut;
}

static RpcResult send_and_wait(RpcProtocol* rpc, Pending* p, int msgType, int contentType,
                               const unsigned char* body, size_t bodyLen, int timeoutMs, const char* method) {
    size_t msgLen;
    unsigned char* msg = build_message(msgType, contentType, p->id, body, bodyLen, &msgLen);
    int ok = ms_send(rpc->ms, msg, msgLen);
    free(msg);
    if (!ok) {
        pending_remove(rpc, p->id);
        RpcResult r = {NULL, json_str("send failed"), 0, NULL};
        return r;
    }
    return wait_pending(rpc, p, timeoutMs, method);
}

static Pending* pending_new(RpcProtocol* rpc) {
    Pending* p = (Pending*)calloc(1, sizeof(Pending));
    new_uuid(p->id);
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->cv, NULL);
    pthread_mutex_lock(&rpc->mu);
    p->next = rpc->pending;
    rpc->pending = p;
    pthread_mutex_unlock(&rpc->mu);
    return p;
}

/* v1.1.3：首个业务请求前自动 HELLO。仅客户端（无 required_token 且持有 auth_token）生效；锁内只握手一次。 */
static RpcResult rpc_ensure_auth(RpcProtocol* rpc) {
    RpcResult ok = {0};
    ok.value = NULL; ok.error = NULL; ok.remote = 0; ok.code = NULL;
    if (rpc->required_token && rpc->required_token[0]) return ok;   /* 服务端角色 */
    if (!rpc->auth_token || !rpc->auth_token[0]) return ok;         /* 无 token，无需握手 */
    pthread_mutex_lock(&rpc->mu);
    int done = rpc->authenticated || rpc->auth_sent;
    pthread_mutex_unlock(&rpc->mu);
    if (done) return ok;

    pthread_mutex_lock(&rpc->auth_mu);
    pthread_mutex_lock(&rpc->mu);
    done = rpc->authenticated || rpc->auth_sent;
    pthread_mutex_unlock(&rpc->mu);
    if (!done) {
        RpcResult hr = rpc_hello(rpc, rpc->default_timeout_ms);
        if (hr.error) {
            pthread_mutex_unlock(&rpc->auth_mu);
            return hr;
        }
        rpc_result_free(&hr);
        pthread_mutex_lock(&rpc->mu);
        rpc->auth_sent = 1;
        pthread_mutex_unlock(&rpc->mu);
    }
    pthread_mutex_unlock(&rpc->auth_mu);
    return ok;
}

int rpc_auth_sent(RpcProtocol* rpc) {
    pthread_mutex_lock(&rpc->mu);
    int v = rpc->auth_sent || rpc->authenticated;
    pthread_mutex_unlock(&rpc->mu);
    return v;
}

RpcResult rpc_send_request(RpcProtocol* rpc, const char* method, JsonValue* params, const char* to) {
    return rpc_send_request_timeout(rpc, method, params, to, rpc->default_timeout_ms);
}

RpcResult rpc_send_request_timeout(RpcProtocol* rpc, const char* method, JsonValue* params, const char* to,
                                   int timeoutMs) {
    RpcResult ar = rpc_ensure_auth(rpc);
    if (ar.error) return ar;
    Pending* p = pending_new(rpc);
    /* 组装 {"method":...,"params":...[,"to":...]}（params 借用，单独 dump） */
    JsonValue* mv = json_str(method);
    char* methodJson = json_dump(mv);
    json_free(mv);
    char* paramsJson = params ? json_dump(params) : NULL;
    if (!paramsJson) { paramsJson = (char*)malloc(5); strcpy(paramsJson, "null"); }
    size_t cap = strlen(methodJson) + strlen(paramsJson) + (to ? strlen(to) + 16 : 0) + 48;
    char* body = (char*)malloc(cap);
    if (to) {
        JsonValue* tv = json_str(to);
        char* toJson = json_dump(tv);
        json_free(tv);
        snprintf(body, cap, "{\"method\":%s,\"params\":%s,\"to\":%s}", methodJson, paramsJson, toJson);
        free(toJson);
    } else {
        snprintf(body, cap, "{\"method\":%s,\"params\":%s}", methodJson, paramsJson);
    }
    free(methodJson);
    free(paramsJson);
    RpcResult r = send_and_wait(rpc, p, TYPE_REQUEST, CONTENT_JSON, (unsigned char*)body, strlen(body),
                                timeoutMs, method);
    free(body);
    return r;
}

RpcResult rpc_send_request_binary(RpcProtocol* rpc, const char* method,
                                  const unsigned char* data, size_t len, JsonValue* argsArr) {
    return rpc_send_request_binary_timeout(rpc, method, data, len, argsArr, rpc->default_timeout_ms);
}

RpcResult rpc_send_request_binary_timeout(RpcProtocol* rpc, const char* method,
                                          const unsigned char* data, size_t len, JsonValue* argsArr,
                                          int timeoutMs) {
    RpcResult ar = rpc_ensure_auth(rpc);
    if (ar.error) return ar;
    Pending* p = pending_new(rpc);
    size_t bodyLen;
    unsigned char* body = encode_binary_request(method, data, len, argsArr, &bodyLen);
    RpcResult r = send_and_wait(rpc, p, TYPE_REQUEST, CONTENT_BINARY, body, bodyLen, timeoutMs, method);
    free(body);
    return r;
}

void rpc_result_free(RpcResult* r) {
    json_free(r->value);
    json_free(r->error);
    r->value = NULL;
    r->error = NULL;
}

int rpc_send_text(RpcProtocol* rpc, const char* text) {
    char id[37];
    new_uuid(id);
    size_t msgLen;
    unsigned char* msg = build_message(TYPE_EVENT, CONTENT_TEXT, id, (const unsigned char*)text, strlen(text), &msgLen);
    int ok = ms_send(rpc->ms, msg, msgLen);
    free(msg);
    return ok;
}

int rpc_send_json(RpcProtocol* rpc, JsonValue* value) {
    char id[37];
    new_uuid(id);
    char* body = json_dump(value);
    size_t msgLen;
    unsigned char* msg = build_message(TYPE_EVENT, CONTENT_JSON, id, (unsigned char*)body, strlen(body), &msgLen);
    int ok = ms_send(rpc->ms, msg, msgLen);
    free(msg);
    free(body);
    return ok;
}

int rpc_send_binary(RpcProtocol* rpc, const unsigned char* data, size_t len) {
    char id[37];
    new_uuid(id);
    size_t msgLen;
    unsigned char* msg = build_message(TYPE_EVENT, CONTENT_BINARY, id, data, len, &msgLen);
    int ok = ms_send(rpc->ms, msg, msgLen);
    free(msg);
    return ok;
}

void rpc_close(RpcProtocol* rpc) {
    pthread_mutex_lock(&rpc->mu);
    if (rpc->closed) {
        pthread_mutex_unlock(&rpc->mu);
        return;
    }
    rpc->closed = 1;
    pthread_cond_broadcast(&rpc->hb_cv);
    pthread_mutex_unlock(&rpc->mu);
    reject_all(rpc, "RPC Protocol destroyed", NULL);
    ms_close(rpc->ms);
}

void rpc_destroy(RpcProtocol* rpc) {
    rpc_close(rpc);
    if (rpc->hb_started) pthread_join(rpc->hb_thread, NULL);
    /* 等待进行中的请求处理线程结束 */
    pthread_mutex_lock(&rpc->mu);
    while (rpc->active > 0) pthread_cond_wait(&rpc->active_cv, &rpc->mu);
    pthread_mutex_unlock(&rpc->mu);
    ms_destroy(rpc->ms);
    HandlerEntry* e = rpc->handlers;
    while (e) {
        HandlerEntry* next = e->next;
        free(e->name);
        free(e);
        e = next;
    }
    free(rpc->auth_token);
    free(rpc->required_token);
    free(rpc->caps);
    free(rpc->impl_name);
    free(rpc->peer_caps);
    free(rpc->server_caps);
    pthread_mutex_destroy(&rpc->mu);
    pthread_mutex_destroy(&rpc->auth_mu);
    pthread_cond_destroy(&rpc->active_cv);
    pthread_cond_destroy(&rpc->hb_cv);
    free(rpc);
}
