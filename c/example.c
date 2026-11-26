/* C 示例：server/client 双模式，用于与其他语言实现互操作。
 * gcc -std=c11 -O2 -I. example.c json.c message_stream.c rpc.c tcp.c -o out/example.exe -lws2_32 -lpthread
 */
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <windows.h>

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "json.h"
#include "message_stream.h"
#include "rpc.h"
#include "tcp.h"

/* ---- 服务端方法（互操作约定）---- */

static JsonValue* h_add(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)n; (void)err;
    return json_num(a[0]->u.number + a[1]->u.number);
}

static JsonValue* deep_copy(JsonValue* v) {
    if (v->type == JSON_BYTES) return json_bytes(v->u.bytes.data, v->u.bytes.len);
    char* s = json_dump(v);
    JsonValue* c = json_parse(s);
    free(s);
    return c;
}

static JsonValue* h_echo(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)n; (void)err;
    return deep_copy(a[0]);
}

static JsonValue* h_greet(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)n; (void)err;
    char buf[256];
    snprintf(buf, sizeof(buf), "hello %s", a[0]->u.str);
    return json_str(buf);
}

static JsonValue* h_blob(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)a; (void)n; (void)err;
    unsigned char b[] = {0, 1, 2, 255};
    return json_bytes(b, sizeof(b));
}

static JsonValue* h_nothing(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)a; (void)n; (void)err;
    return NULL;
}

static JsonValue* h_fail(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)a; (void)n;
    *err = _strdup("boom");
    return NULL;
}

static JsonValue* h_upload(void* u, JsonValue** a, size_t n, char** err) {
    (void)u; (void)n; (void)err;
    JsonValue* head = json_arr();
    for (size_t i = 0; i < 4 && i < a[0]->u.bytes.len; i++)
        json_arr_push(head, json_num(a[0]->u.bytes.data[i]));
    JsonValue* r = json_obj();
    json_obj_set(r, "size", json_num((double)a[0]->u.bytes.len));
    json_obj_set(r, "tag", deep_copy(a[1]));
    json_obj_set(r, "head", head);
    return r;
}

static JsonValue* h_push(void* u, JsonValue** a, size_t n, char** err) {
    (void)a; (void)n; (void)err;
    RpcProtocol* rpc = (RpcProtocol*)u;
    JsonValue* ev = json_obj();
    json_obj_set(ev, "push", json_num(1));
    rpc_send_json(rpc, ev);
    json_free(ev);
    rpc_send_text(rpc, "srv-text");
    unsigned char b[] = {1, 2};
    rpc_send_binary(rpc, b, sizeof(b));
    return json_str("sent");
}

static void silent_error(void* u, const char* msg, const char* code) { (void)u; (void)msg; (void)code; }

static void register_methods(RpcProtocol* rpc) {
    rpc_register(rpc, "add", h_add, NULL);
    rpc_register(rpc, "echo", h_echo, NULL);
    rpc_register(rpc, "greet", h_greet, NULL);
    rpc_register(rpc, "blob", h_blob, NULL);
    rpc_register(rpc, "nothing", h_nothing, NULL);
    rpc_register(rpc, "fail", h_fail, NULL);
    rpc_register(rpc, "upload", h_upload, NULL);
    rpc_register(rpc, "pushEvents", h_push, rpc);
}

typedef struct { TcpConn* conn; } ConnArg;

static void* serve_conn(void* arg) {
    ConnArg* ca = (ConnArg*)arg;
    RpcProtocol* rpc = rpc_create(ms_create(tcp_io(ca->conn), 1, 0));
    rpc_on_error(rpc, silent_error, NULL);
    register_methods(rpc);
    free(ca);
    /* 保持连接存活 */
    Sleep(24 * 3600 * 1000);
    rpc_destroy(rpc);
    return NULL;
}

static void run_server(unsigned short port) {
    TcpListener* ln = tcp_listen("127.0.0.1", port);
    if (!ln) { fprintf(stderr, "listen failed\n"); exit(1); }
    printf("{\"port\":%u}\n", tcp_listener_port(ln));
    fflush(stdout);
    for (;;) {
        TcpConn* c = tcp_accept(ln);
        if (!c) break;
        ConnArg* ca = (ConnArg*)malloc(sizeof(ConnArg));
        ca->conn = c;
        pthread_t t;
        pthread_create(&t, NULL, serve_conn, ca);
        pthread_detach(t);
    }
}

/* ---- 客户端 ---- */

static int passed = 0;

static void check(const char* name, int ok) {
    if (ok) { passed++; printf("PASS %s\n", name); }
    else { printf("FAIL %s\n", name); exit(1); }
}

typedef struct {
    JsonValue* items[8];
    int count;
} EventSink;

static void collect_event(void* u, int ct, JsonValue* content) {
    (void)ct;
    EventSink* sink = (EventSink*)u;
    if (sink->count < 8) {
        if (content->type == JSON_BYTES) {
            sink->items[sink->count++] = json_bytes(content->u.bytes.data, content->u.bytes.len);
        } else {
            char* s = json_dump(content);
            sink->items[sink->count++] = json_parse(s);
            free(s);
        }
    }
}

typedef struct { RpcProtocol* rpc; int i; int ok; } ConcArg;

static void* conc_fn(void* a) {
    ConcArg* ca = (ConcArg*)a;
    JsonValue* p = json_arr();
    json_arr_push(p, json_num(ca->i));
    json_arr_push(p, json_num(ca->i));
    RpcResult r = rpc_send_request(ca->rpc, "add", p, NULL);
    ca->ok = r.value && r.value->u.number == ca->i * 2;
    rpc_result_free(&r);
    json_free(p);
    return NULL;
}

static void run_client(unsigned short port) {
    TcpConn* conn = tcp_connect("127.0.0.1", port);
    if (!conn) { fprintf(stderr, "connect failed\n"); exit(1); }
    RpcProtocol* rpc = rpc_create(ms_create(tcp_io(conn), 1, 0));
    rpc_on_error(rpc, silent_error, NULL);

    JsonValue* p = json_arr();
    json_arr_push(p, json_num(3));
    json_arr_push(p, json_num(4));
    RpcResult r = rpc_send_request(rpc, "add", p, NULL);
    check("add", r.value && r.value->u.number == 7);
    rpc_result_free(&r);
    json_free(p);

    JsonValue* list = json_arr();
    json_arr_push(list, json_num(1));
    json_arr_push(list, json_num(2.5));
    json_arr_push(list, json_bool(1));
    json_arr_push(list, json_null());
    JsonValue* obj = json_obj();
    json_obj_set(obj, "name", json_str("测试"));
    json_obj_set(obj, "list", list);
    r = rpc_send_request(rpc, "echo", obj, NULL);
    JsonValue* name = json_get(r.value, "name");
    JsonValue* rl = json_get(r.value, "list");
    check("echo", name && strcmp(name->u.str, "测试") == 0 && rl && rl->u.arr.items[1]->u.number == 2.5);
    rpc_result_free(&r);
    json_free(obj);

    JsonValue* world = json_str("世界");
    r = rpc_send_request(rpc, "greet", world, NULL);
    check("greet", r.value && strcmp(r.value->u.str, "hello 世界") == 0);
    rpc_result_free(&r);
    json_free(world);

    r = rpc_send_request(rpc, "blob", NULL, NULL);
    unsigned char expectBlob[] = {0, 1, 2, 255};
    check("blob", r.value && r.value->type == JSON_BYTES && r.value->u.bytes.len == 4 &&
                  memcmp(r.value->u.bytes.data, expectBlob, 4) == 0);
    rpc_result_free(&r);

    r = rpc_send_request(rpc, "nothing", NULL, NULL);
    check("nothing", r.value && r.value->type == JSON_NULL);
    rpc_result_free(&r);

    r = rpc_send_request(rpc, "fail", NULL, NULL);
    check("fail", r.remote && r.error && r.error->type == JSON_STR && strcmp(r.error->u.str, "boom") == 0);
    rpc_result_free(&r);

    r = rpc_send_request(rpc, "nope", NULL, NULL);
    check("methodNotFound", r.remote && r.error && r.error->type == JSON_STR &&
                            strcmp(r.error->u.str, "Method not found: nope") == 0);
    rpc_result_free(&r);

    size_t n = 256 * 300;
    unsigned char* data = (unsigned char*)malloc(n);
    for (size_t i = 0; i < n; i++) data[i] = (unsigned char)(i & 0xFF);
    JsonValue* args = json_arr();
    json_arr_push(args, json_str("tag-1"));
    r = rpc_send_request_binary(rpc, "upload", data, n, args);
    JsonValue* size = json_get(r.value, "size");
    JsonValue* tag = json_get(r.value, "tag");
    JsonValue* head = json_get(r.value, "head");
    check("upload", size && size->u.number == (double)n && tag && strcmp(tag->u.str, "tag-1") == 0 &&
                  head && head->u.arr.len == 4 && head->u.arr.items[3]->u.number == 3);
    rpc_result_free(&r);
    json_free(args);
    free(data);

    EventSink sink = {0};
    rpc_on_event(rpc, collect_event, &sink);
    r = rpc_send_request(rpc, "pushEvents", NULL, NULL);
    rpc_result_free(&r);
    for (int i = 0; i < 300 && sink.count < 3; i++) Sleep(10);
    unsigned char expectEv[] = {1, 2};
    check("events", sink.count == 3 &&
                  json_get(sink.items[0], "push") && json_get(sink.items[0], "push")->u.number == 1 &&
                  sink.items[1]->type == JSON_STR && strcmp(sink.items[1]->u.str, "srv-text") == 0 &&
                  sink.items[2]->type == JSON_BYTES && sink.items[2]->u.bytes.len == 2 &&
                  memcmp(sink.items[2]->u.bytes.data, expectEv, 2) == 0);
    for (int i = 0; i < sink.count; i++) json_free(sink.items[i]);

    ConcArg cargs[50];
    pthread_t threads[50];
    for (int i = 0; i < 50; i++) {
        cargs[i] = (ConcArg){rpc, i, 0};
        pthread_create(&threads[i], NULL, conc_fn, &cargs[i]);
    }
    int allOk = 1;
    for (int i = 0; i < 50; i++) {
        pthread_join(threads[i], NULL);
        if (!cargs[i].ok) allOk = 0;
    }
    check("concurrent50", allOk);

    JsonValue* routed = json_str("routed");
    r = rpc_send_request(rpc, "echo", routed, "some-target");
    check("toField", r.value && r.value->type == JSON_STR && strcmp(r.value->u.str, "routed") == 0);
    rpc_result_free(&r);
    json_free(routed);

    printf("ALL %d CHECKS PASSED\n", passed);
    rpc_destroy(rpc);
    tcp_conn_free(conn);
    exit(0);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "usage: example server|client <port>\n");
        return 2;
    }
    unsigned short port = (unsigned short)atoi(argv[2]);
    if (strcmp(argv[1], "server") == 0) run_server(port);
    else if (strcmp(argv[1], "client") == 0) run_client(port);
    else return 2;
    return 0;
}