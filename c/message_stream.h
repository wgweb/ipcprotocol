/* MessageStream 帧层（C11 + pthreads）：长度头分帧 + 可选 CRC32 + 错误包与重同步。
 * 回调必须在 ms_start 之前设置；rpc_create 会自动处理。 */
#ifndef IPC_MESSAGE_STREAM_H
#define IPC_MESSAGE_STREAM_H

#include <stddef.h>

#define IPC_ERR_LENGTH_CRC_FAILED 1
#define IPC_ERR_MESSAGE_CRC_FAILED  2
#define IPC_ERR_BUFFER_OVERFLOW     3

/* 传输抽象：read 返回字节数（0=EOF，-1=错误）；write 返回 1 成功 0 失败 */
typedef struct {
    int (*read)(void* ctx, unsigned char* buf, int max);
    int (*write)(void* ctx, const unsigned char* data, size_t len);
    void (*close)(void* ctx);
    void* ctx;
} IpcIo;

typedef struct MessageStream MessageStream;

MessageStream* ms_create(IpcIo io, int enableCRC, size_t maxBufferSize);
void ms_on_message(MessageStream* ms, void (*cb)(void* user, const unsigned char* data, size_t len), void* user);
/* code 为静态错误码字符串（如 "ERESYNC_TIMEOUT"），可能为 NULL；不需释放 */
void ms_on_error(MessageStream* ms, void (*cb)(void* user, const char* msg, const char* code), void* user);
void ms_on_sync_complete(MessageStream* ms, void (*cb)(void* user), void* user);
void ms_start(MessageStream* ms);   /* 幂等 */
/* 重同步握手超时（毫秒），默认 3000；<=0 关闭。须在 ms_start 前设置。 */
void ms_set_sync_timeout(MessageStream* ms, int timeoutMs);
int ms_is_closed(MessageStream* ms);
/* 业务发送：协议模式下门控等待重同步完成；关闭后返回 0 */
int ms_send(MessageStream* ms, const unsigned char* data, size_t len);
int ms_send_error(MessageStream* ms, unsigned char code);      /* 旁路门控 */
int ms_send_protocol(MessageStream* ms, const char* data, size_t len); /* 旁路门控 */
void ms_close(MessageStream* ms);
void ms_destroy(MessageStream* ms); /* close + 等读线程/计时线程退出 + 释放 */

unsigned ipc_crc32(const unsigned char* data, size_t len);

#endif