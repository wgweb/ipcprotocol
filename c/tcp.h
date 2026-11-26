/* TCP 传输封装（Windows/Winsock）。其他平台可替换为自行实现的 IpcIo。 */
#ifndef IPC_TCP_H
#define IPC_TCP_H

#include "message_stream.h"

typedef struct TcpConn TcpConn;
typedef struct TcpListener TcpListener;

TcpConn* tcp_connect(const char* host, unsigned short port);
IpcIo tcp_io(TcpConn* c);       /* 借用 c；close 回调会关闭 socket */
void tcp_conn_free(TcpConn* c); /* 关闭并释放 */

TcpListener* tcp_listen(const char* host, unsigned short port);
unsigned short tcp_listener_port(TcpListener* l);
TcpConn* tcp_accept(TcpListener* l);
void tcp_listener_free(TcpListener* l);

#endif