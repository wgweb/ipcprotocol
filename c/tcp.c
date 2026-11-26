#include "tcp.h"

#include <stdlib.h>
#include <string.h>

/* ---- v1.1.3：跨平台 socket 适配（Windows Winsock / POSIX） ---- */
#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
typedef SOCKET ipc_sock_t;
#define IPC_INVALID_SOCK INVALID_SOCKET
#define IPC_SHUT_BOTH SD_BOTH
#define ipc_close_sock closesocket
#define IPC_SEND_FLAGS 0
#define IPC_SOCK_ERR SOCKET_ERROR
static void wsa_init(void) {
    static int done = 0;
    if (!done) { WSADATA data; WSAStartup(MAKEWORD(2, 2), &data); done = 1; }
}
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>
typedef int ipc_sock_t;
#define IPC_INVALID_SOCK (-1)
#define ipc_close_sock close
#define IPC_SEND_FLAGS MSG_NOSIGNAL
#define IPC_SOCK_ERR (-1)
#define IPC_SHUT_BOTH SHUT_RDWR
static void wsa_init(void) { }
#endif

struct TcpConn { ipc_sock_t s; };
struct TcpListener { ipc_sock_t s; unsigned short port; };

static int io_read(void* ctx, unsigned char* buf, int max) {
    TcpConn* c = (TcpConn*)ctx;
    int n = recv(c->s, (char*)buf, (size_t)max, 0);
    return n == IPC_SOCK_ERR ? -1 : n;
}

static int io_write(void* ctx, const unsigned char* data, size_t len) {
    TcpConn* c = (TcpConn*)ctx;
    size_t off = 0;
    while (off < len) {
        int n = send(c->s, (const char*)(data + off), (size_t)(len - off), IPC_SEND_FLAGS);
        if (n == IPC_SOCK_ERR || n == 0) return 0;
        off += (size_t)n;
    }
    return 1;
}

static void io_close(void* ctx) {
    TcpConn* c = (TcpConn*)ctx;
    if (c->s != IPC_INVALID_SOCK) {
        shutdown(c->s, IPC_SHUT_BOTH);
        ipc_close_sock(c->s);
        c->s = IPC_INVALID_SOCK;
    }
}

IpcIo tcp_io(TcpConn* c) {
    IpcIo io;
    io.read = io_read;
    io.write = io_write;
    io.close = io_close;
    io.ctx = c;
    return io;
}

TcpConn* tcp_connect(const char* host, unsigned short port) {
    wsa_init();
    ipc_sock_t s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == IPC_INVALID_SOCK) return NULL;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);
    if (connect(s, (struct sockaddr*)&addr, sizeof(addr)) == IPC_SOCK_ERR) {
        ipc_close_sock(s);
        return NULL;
    }
    TcpConn* c = (TcpConn*)malloc(sizeof(TcpConn));
    c->s = s;
    return c;
}

void tcp_conn_free(TcpConn* c) {
    if (!c) return;
    io_close(c);
    free(c);
}

TcpListener* tcp_listen(const char* host, unsigned short port) {
    wsa_init();
    ipc_sock_t s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (s == IPC_INVALID_SOCK) return NULL;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);
    if (bind(s, (struct sockaddr*)&addr, sizeof(addr)) == IPC_SOCK_ERR || listen(s, 16) == IPC_SOCK_ERR) {
        ipc_close_sock(s);
        return NULL;
    }
    socklen_t len = sizeof(addr);
    getsockname(s, (struct sockaddr*)&addr, &len);
    TcpListener* l = (TcpListener*)malloc(sizeof(TcpListener));
    l->s = s;
    l->port = ntohs(addr.sin_port);
    return l;
}

unsigned short tcp_listener_port(TcpListener* l) { return l->port; }

TcpConn* tcp_accept(TcpListener* l) {
    ipc_sock_t s = accept(l->s, NULL, NULL);
    if (s == IPC_INVALID_SOCK) return NULL;
    TcpConn* c = (TcpConn*)malloc(sizeof(TcpConn));
    c->s = s;
    return c;
}

void tcp_listener_free(TcpListener* l) {
    if (!l) return;
    if (l->s != IPC_INVALID_SOCK) ipc_close_sock(l->s);
    free(l);
}