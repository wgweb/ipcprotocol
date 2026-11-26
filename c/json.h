/* 零依赖 JSON 编解码（C11），语义对齐 JS JSON.stringify/parse。
 * JSON_BYTES 仅用于顶层载荷/参数；序列化时输出数字数组（对齐 JSON.stringify(Buffer)）。
 * 所有权约定：构造/解析返回的值由调用方 json_free；容器 push/set 接管子值所有权。 */
#ifndef IPC_JSON_H
#define IPC_JSON_H

#include <stddef.h>

typedef enum {
    JSON_NULL, JSON_BOOL, JSON_NUM, JSON_STR, JSON_ARR, JSON_OBJ, JSON_BYTES
} JsonType;

typedef struct JsonValue JsonValue;
struct JsonValue {
    JsonType type;
    union {
        int boolean;
        double number;
        char* str;
        struct { JsonValue** items; size_t len, cap; } arr;
        struct { char** keys; JsonValue** vals; size_t len, cap; } obj;
        struct { unsigned char* data; size_t len; } bytes;
    } u;
};

JsonValue* json_null(void);
JsonValue* json_bool(int b);
JsonValue* json_num(double n);
JsonValue* json_str(const char* s);          /* 拷贝 */
JsonValue* json_arr(void);
void json_arr_push(JsonValue* arr, JsonValue* item); /* 接管 item */
JsonValue* json_obj(void);
void json_obj_set(JsonValue* obj, const char* key, JsonValue* val); /* 接管 val */
JsonValue* json_bytes(const unsigned char* data, size_t len); /* 拷贝 */
void json_free(JsonValue* v);

JsonValue* json_get(const JsonValue* obj, const char* key); /* 借用，不存在返回 NULL */

/* JS 真值语义 */
int json_truthy(const JsonValue* v);

char* json_dump(const JsonValue* v);   /* malloc 字符串，调用方 free */
JsonValue* json_parse(const char* s);  /* 失败返回 NULL */

#endif