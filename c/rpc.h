/* RpcProtocol 消息层（C11 + pthreads）：请求/响应/事件，与 RpcProtocol.js 线格式兼容。 */
#ifndef IPC_RPC_H
#define IPC_RPC_H

#include "json.h"
#include "message_stream.h"

typedef struct RpcProtocol RpcProtocol;

/* 服务端方法处理器。
 * args 为借用指针数组；返回值为接管所有权的结果（NULL 视为 JSON null）。
 * 出错时返回 NULL 并设置 *errMsg（malloc 字符串，rpc 层负责 free）。 */
typedef JsonValue* (*IpcHandler)(void* user, JsonValue** args, size_t nargs, char** errMsg);

typedef struct {
    JsonValue* value; /* 成功结果（所有权归调用方） */
    JsonValue* error; /* 失败时的错误值：remote=1 为对端原始 error 值，否则为 JSON_STR 消息 */
    int remote;
    const char* code; /* 错误码静态字符串（如 "ETIMEDOUT"），NULL 表示未指定；不需释放 */
} RpcResult;

/* 接管 ms 所有权；先注册回调再启动读循环。默认心跳 5s/15s，不鉴权。 */
RpcProtocol* rpc_create(MessageStream* ms);
/* heartbeatIntervalMs<=0 关闭心跳；requiredToken 非空时未通过 HELLO 鉴权的请求被拒绝。
 * authToken 为发起 HELLO 时携带的令牌；requiredToken 为服务端要求的令牌。 */
RpcProtocol* rpc_create_options(MessageStream* ms, int heartbeatIntervalMs, int heartbeatTimeoutMs,
                                const char* authToken, const char* requiredToken);
void rpc_register(RpcProtocol* rpc, const char* name, IpcHandler h, void* user);
/* 事件回调中 content 为借用，回调返回后释放 */
void rpc_on_event(RpcProtocol* rpc, void (*cb)(void* user, int contentType, JsonValue* content), void* user);
/* code 为静态错误码字符串（如 "EHEARTBEAT"），可能为 NULL；不需释放 */
void rpc_on_error(RpcProtocol* rpc, void (*cb)(void* user, const char* msg, const char* code), void* user);

/* 发起 HELLO 握手（版本协商 + 可选鉴权）。timeoutMs<0 用默认超时，0 关闭超时。
 * 成功返回 value=ACK 对象；ACK error → code="EAUTH"，版本不符 → code="EVERSION"。 */
RpcResult rpc_hello(RpcProtocol* rpc, int timeoutMs);
/* v1.1.3：是否已完成（或被跳过）自动握手 */
int rpc_auth_sent(RpcProtocol* rpc);

/* v1.1.2 能力协商（PROTOCOL.md §8.4）：返回逗号分隔的能力串（借用，勿释放）。
 * peer_caps / server_caps 为 NULL 表示对端未声明（旧实现）。 */
const char* rpc_capabilities(RpcProtocol* rpc);
const char* rpc_peer_capabilities(RpcProtocol* rpc);
const char* rpc_server_capabilities(RpcProtocol* rpc);
/* 因能力协商停用心跳的原因；NULL 表示心跳正常。 */
const char* rpc_heartbeat_disabled_reason(RpcProtocol* rpc);
/* 覆盖本端能力声明与实现名（HELLO/HELLO_ACK 使用）。 */
void rpc_set_capabilities(RpcProtocol* rpc, const char* capsCsv, const char* implName);

/* 默认 RPC 超时（毫秒），对齐 JS 修复版 15s；0 表示关闭。 */
void rpc_set_default_timeout(RpcProtocol* rpc, int timeoutMs);

/* params 为借用（NULL 视为 null）；to 可为 NULL。阻塞等待响应（默认超时）。 */
RpcResult rpc_send_request(RpcProtocol* rpc, const char* method, JsonValue* params, const char* to);
/* 带显式超时（毫秒）的请求；0 关闭超时。超时返回 error 消息且 code="ETIMEDOUT"。 */
RpcResult rpc_send_request_timeout(RpcProtocol* rpc, const char* method, JsonValue* params, const char* to,
                                   int timeoutMs);
/* argsArr 为借用的 JSON_ARR（可为 NULL）；handler 实参为 [Bytes(data), ...args] */
RpcResult rpc_send_request_binary(RpcProtocol* rpc, const char* method,
                                  const unsigned char* data, size_t len, JsonValue* argsArr);
/* 带显式超时（毫秒）的二进制请求；0 关闭超时。 */
RpcResult rpc_send_request_binary_timeout(RpcProtocol* rpc, const char* method,
                                          const unsigned char* data, size_t len, JsonValue* argsArr,
                                          int timeoutMs);
void rpc_result_free(RpcResult* r);

int rpc_send_text(RpcProtocol* rpc, const char* text);
int rpc_send_json(RpcProtocol* rpc, JsonValue* value); /* 借用 */
int rpc_send_binary(RpcProtocol* rpc, const unsigned char* data, size_t len);

void rpc_close(RpcProtocol* rpc);   /* 拒绝所有 pending 并关闭流 */
void rpc_destroy(RpcProtocol* rpc); /* close + 等请求处理完 + 释放（含 ms） */

#endif