#include "json.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char* dup_str(const char* s) {
    size_t n = strlen(s) + 1;
    char* p = (char*)malloc(n);
    memcpy(p, s, n);
    return p;
}

/* ---- 构造 ---- */

static JsonValue* alloc_value(JsonType t) {
    JsonValue* v = (JsonValue*)calloc(1, sizeof(JsonValue));
    v->type = t;
    return v;
}

JsonValue* json_null(void) { return alloc_value(JSON_NULL); }
JsonValue* json_bool(int b) { JsonValue* v = alloc_value(JSON_BOOL); v->u.boolean = b ? 1 : 0; return v; }
JsonValue* json_num(double n) { JsonValue* v = alloc_value(JSON_NUM); v->u.number = n; return v; }

JsonValue* json_str(const char* s) {
    JsonValue* v = alloc_value(JSON_STR);
    v->u.str = dup_str(s ? s : "");
    return v;
}

JsonValue* json_arr(void) { return alloc_value(JSON_ARR); }

void json_arr_push(JsonValue* arr, JsonValue* item) {
    if (arr->u.arr.len == arr->u.arr.cap) {
        arr->u.arr.cap = arr->u.arr.cap ? arr->u.arr.cap * 2 : 8;
        arr->u.arr.items = (JsonValue**)realloc(arr->u.arr.items, arr->u.arr.cap * sizeof(JsonValue*));
    }
    arr->u.arr.items[arr->u.arr.len++] = item;
}

JsonValue* json_obj(void) { return alloc_value(JSON_OBJ); }

void json_obj_set(JsonValue* obj, const char* key, JsonValue* val) {
    for (size_t i = 0; i < obj->u.obj.len; i++) {
        if (strcmp(obj->u.obj.keys[i], key) == 0) {
            json_free(obj->u.obj.vals[i]);
            obj->u.obj.vals[i] = val;
            return;
        }
    }
    if (obj->u.obj.len == obj->u.obj.cap) {
        obj->u.obj.cap = obj->u.obj.cap ? obj->u.obj.cap * 2 : 8;
        obj->u.obj.keys = (char**)realloc(obj->u.obj.keys, obj->u.obj.cap * sizeof(char*));
        obj->u.obj.vals = (JsonValue**)realloc(obj->u.obj.vals, obj->u.obj.cap * sizeof(JsonValue*));
    }
    obj->u.obj.keys[obj->u.obj.len] = dup_str(key);
    obj->u.obj.vals[obj->u.obj.len] = val;
    obj->u.obj.len++;
}

JsonValue* json_bytes(const unsigned char* data, size_t len) {
    JsonValue* v = alloc_value(JSON_BYTES);
    v->u.bytes.data = (unsigned char*)malloc(len ? len : 1);
    memcpy(v->u.bytes.data, data, len);
    v->u.bytes.len = len;
    return v;
}

void json_free(JsonValue* v) {
    if (!v) return;
    switch (v->type) {
        case JSON_STR: free(v->u.str); break;
        case JSON_ARR:
            for (size_t i = 0; i < v->u.arr.len; i++) json_free(v->u.arr.items[i]);
            free(v->u.arr.items);
            break;
        case JSON_OBJ:
            for (size_t i = 0; i < v->u.obj.len; i++) {
                free(v->u.obj.keys[i]);
                json_free(v->u.obj.vals[i]);
            }
            free(v->u.obj.keys);
            free(v->u.obj.vals);
            break;
        case JSON_BYTES: free(v->u.bytes.data); break;
        default: break;
    }
    free(v);
}

JsonValue* json_get(const JsonValue* obj, const char* key) {
    if (!obj || obj->type != JSON_OBJ) return NULL;
    for (size_t i = 0; i < obj->u.obj.len; i++) {
        if (strcmp(obj->u.obj.keys[i], key) == 0) return obj->u.obj.vals[i];
    }
    return NULL;
}

int json_truthy(const JsonValue* v) {
    if (!v) return 0;
    switch (v->type) {
        case JSON_NULL: return 0;
        case JSON_BOOL: return v->u.boolean;
        case JSON_NUM: return v->u.number != 0.0 && !isnan(v->u.number);
        case JSON_STR: return v->u.str[0] != '\0';
        default: return 1;
    }
}

/* ---- 序列化 ---- */

typedef struct { char* data; size_t len, cap; } Sb;

static void sb_reserve(Sb* sb, size_t extra) {
    if (sb->len + extra + 1 > sb->cap) {
        while (sb->len + extra + 1 > sb->cap) sb->cap = sb->cap ? sb->cap * 2 : 128;
        sb->data = (char*)realloc(sb->data, sb->cap);
    }
}

static void sb_puts(Sb* sb, const char* s) {
    size_t n = strlen(s);
    sb_reserve(sb, n);
    memcpy(sb->data + sb->len, s, n);
    sb->len += n;
    sb->data[sb->len] = '\0';
}

static void sb_putc(Sb* sb, char c) {
    sb_reserve(sb, 1);
    sb->data[sb->len++] = c;
    sb->data[sb->len] = '\0';
}

static void sb_quote(Sb* sb, const char* s) {
    sb_putc(sb, '"');
    for (const unsigned char* p = (const unsigned char*)s; *p; p++) {
        unsigned char c = *p;
        switch (c) {
            case '"': sb_puts(sb, "\\\""); break;
            case '\\': sb_puts(sb, "\\\\"); break;
            case '\n': sb_puts(sb, "\\n"); break;
            case '\r': sb_puts(sb, "\\r"); break;
            case '\t': sb_puts(sb, "\\t"); break;
            case '\b': sb_puts(sb, "\\b"); break;
            case '\f': sb_puts(sb, "\\f"); break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    snprintf(buf, sizeof(buf), "\\u%04x", c);
                    sb_puts(sb, buf);
                } else {
                    sb_putc(sb, (char)c); /* UTF-8 原样输出 */
                }
        }
    }
    sb_putc(sb, '"');
}

static void dump_value(Sb* sb, const JsonValue* v) {
    char buf[64];
    switch (v->type) {
        case JSON_NULL: sb_puts(sb, "null"); break;
        case JSON_BOOL: sb_puts(sb, v->u.boolean ? "true" : "false"); break;
        case JSON_NUM:
            if (isnan(v->u.number) || isinf(v->u.number)) {
                sb_puts(sb, "null");
            } else if (v->u.number == floor(v->u.number) && fabs(v->u.number) < 1e15) {
                snprintf(buf, sizeof(buf), "%.0f", v->u.number);
                sb_puts(sb, buf);
            } else {
                snprintf(buf, sizeof(buf), "%.17g", v->u.number);
                sb_puts(sb, buf);
            }
            break;
        case JSON_STR: sb_quote(sb, v->u.str); break;
        case JSON_BYTES:
            sb_putc(sb, '[');
            for (size_t i = 0; i < v->u.bytes.len; i++) {
                if (i) sb_putc(sb, ',');
                snprintf(buf, sizeof(buf), "%u", v->u.bytes.data[i]);
                sb_puts(sb, buf);
            }
            sb_putc(sb, ']');
            break;
        case JSON_ARR:
            sb_putc(sb, '[');
            for (size_t i = 0; i < v->u.arr.len; i++) {
                if (i) sb_putc(sb, ',');
                dump_value(sb, v->u.arr.items[i]);
            }
            sb_putc(sb, ']');
            break;
        case JSON_OBJ:
            sb_putc(sb, '{');
            for (size_t i = 0; i < v->u.obj.len; i++) {
                if (i) sb_putc(sb, ',');
                sb_quote(sb, v->u.obj.keys[i]);
                sb_putc(sb, ':');
                dump_value(sb, v->u.obj.vals[i]);
            }
            sb_putc(sb, '}');
            break;
    }
}

char* json_dump(const JsonValue* v) {
    Sb sb = {0};
    dump_value(&sb, v);
    if (!sb.data) { sb.data = (char*)malloc(1); sb.data[0] = '\0'; }
    return sb.data;
}

/* ---- 解析 ---- */

/* 递归深度上限：防止恶意深嵌套 JSON 打爆栈（超限返回 NULL 走解析失败路径） */
#define JSON_MAX_DEPTH 64

typedef struct { const char* s; size_t i, n; int depth; } Parser;

static JsonValue* parse_value(Parser* p);

static void skip_ws(Parser* p) {
    while (p->i < p->n) {
        char c = p->s[p->i];
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') p->i++;
        else break;
    }
}

static int hex4(Parser* p, unsigned* out) {
    if (p->i + 4 > p->n) return 0;
    unsigned v = 0;
    for (int k = 0; k < 4; k++) {
        char c = p->s[p->i++];
        v <<= 4;
        if (c >= '0' && c <= '9') v |= (unsigned)(c - '0');
        else if (c >= 'a' && c <= 'f') v |= (unsigned)(c - 'a' + 10);
        else if (c >= 'A' && c <= 'F') v |= (unsigned)(c - 'A' + 10);
        else return 0;
    }
    *out = v;
    return 1;
}

static void append_utf8(Sb* sb, unsigned cp) {
    if (cp < 0x80) {
        sb_putc(sb, (char)cp);
    } else if (cp < 0x800) {
        sb_putc(sb, (char)(0xC0 | (cp >> 6)));
        sb_putc(sb, (char)(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        sb_putc(sb, (char)(0xE0 | (cp >> 12)));
        sb_putc(sb, (char)(0x80 | ((cp >> 6) & 0x3F)));
        sb_putc(sb, (char)(0x80 | (cp & 0x3F)));
    } else {
        sb_putc(sb, (char)(0xF0 | (cp >> 18)));
        sb_putc(sb, (char)(0x80 | ((cp >> 12) & 0x3F)));
        sb_putc(sb, (char)(0x80 | ((cp >> 6) & 0x3F)));
        sb_putc(sb, (char)(0x80 | (cp & 0x3F)));
    }
}

static char* parse_string(Parser* p) {
    if (p->i >= p->n || p->s[p->i] != '"') return NULL;
    p->i++;
    Sb sb = {0};
    for (;;) {
        if (p->i >= p->n) { free(sb.data); return NULL; }
        unsigned char c = (unsigned char)p->s[p->i++];
        if (c == '"') {
            if (!sb.data) { sb.data = (char*)malloc(1); sb.data[0] = '\0'; }
            return sb.data;
        }
        if (c != '\\') { sb_putc(&sb, (char)c); continue; }
        if (p->i >= p->n) { free(sb.data); return NULL; }
        char e = p->s[p->i++];
        switch (e) {
            case '"': sb_putc(&sb, '"'); break;
            case '\\': sb_putc(&sb, '\\'); break;
            case '/': sb_putc(&sb, '/'); break;
            case 'b': sb_putc(&sb, '\b'); break;
            case 'f': sb_putc(&sb, '\f'); break;
            case 'n': sb_putc(&sb, '\n'); break;
            case 'r': sb_putc(&sb, '\r'); break;
            case 't': sb_putc(&sb, '\t'); break;
            case 'u': {
                unsigned hi;
                if (!hex4(p, &hi)) { free(sb.data); return NULL; }
                unsigned cp = hi;
                if (hi >= 0xD800 && hi <= 0xDBFF) {
                    if (p->i + 2 > p->n || p->s[p->i] != '\\' || p->s[p->i + 1] != 'u') {
                        free(sb.data); return NULL;
                    }
                    p->i += 2;
                    unsigned lo;
                    if (!hex4(p, &lo) || lo < 0xDC00 || lo > 0xDFFF) { free(sb.data); return NULL; }
                    cp = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00);
                }
                append_utf8(&sb, cp);
                break;
            }
            default: free(sb.data); return NULL;
        }
    }
}

static JsonValue* parse_object(Parser* p) {
    p->i++; /* '{' */
    JsonValue* obj = json_obj();
    skip_ws(p);
    if (p->i < p->n && p->s[p->i] == '}') { p->i++; return obj; }
    for (;;) {
        skip_ws(p);
        char* key = parse_string(p);
        if (!key) { json_free(obj); return NULL; }
        skip_ws(p);
        if (p->i >= p->n || p->s[p->i] != ':') { free(key); json_free(obj); return NULL; }
        p->i++;
        skip_ws(p);
        JsonValue* val = parse_value(p);
        if (!val) { free(key); json_free(obj); return NULL; }
        json_obj_set(obj, key, val);
        free(key);
        skip_ws(p);
        if (p->i < p->n && p->s[p->i] == ',') { p->i++; continue; }
        if (p->i < p->n && p->s[p->i] == '}') { p->i++; return obj; }
        json_free(obj);
        return NULL;
    }
}

static JsonValue* parse_array(Parser* p) {
    p->i++; /* '[' */
    JsonValue* arr = json_arr();
    skip_ws(p);
    if (p->i < p->n && p->s[p->i] == ']') { p->i++; return arr; }
    for (;;) {
        skip_ws(p);
        JsonValue* item = parse_value(p);
        if (!item) { json_free(arr); return NULL; }
        json_arr_push(arr, item);
        skip_ws(p);
        if (p->i < p->n && p->s[p->i] == ',') { p->i++; continue; }
        if (p->i < p->n && p->s[p->i] == ']') { p->i++; return arr; }
        json_free(arr);
        return NULL;
    }
}

static JsonValue* parse_value(Parser* p) {
    skip_ws(p);
    if (p->i >= p->n) return NULL;
    char c = p->s[p->i];
    if (c == '{' || c == '[') {
        if (p->depth >= JSON_MAX_DEPTH) return NULL;
        p->depth++;
        JsonValue* v = (c == '{') ? parse_object(p) : parse_array(p);
        p->depth--;
        return v;
    }
    if (c == '"') {
        char* s = parse_string(p);
        if (!s) return NULL;
        JsonValue* v = alloc_value(JSON_STR);
        v->u.str = s;
        return v;
    }
    if (p->n - p->i >= 4 && strncmp(p->s + p->i, "true", 4) == 0) { p->i += 4; return json_bool(1); }
    if (p->n - p->i >= 5 && strncmp(p->s + p->i, "false", 5) == 0) { p->i += 5; return json_bool(0); }
    if (p->n - p->i >= 4 && strncmp(p->s + p->i, "null", 4) == 0) { p->i += 4; return json_null(); }
    if (c == '-' || (c >= '0' && c <= '9')) {
        size_t start = p->i;
        while (p->i < p->n) {
            char d = p->s[p->i];
            if ((d >= '0' && d <= '9') || d == '-' || d == '+' || d == '.' || d == 'e' || d == 'E') p->i++;
            else break;
        }
        char* end = NULL;
        double n = strtod(p->s + start, &end);
        if (end != p->s + p->i) return NULL;
        return json_num(n);
    }
    return NULL;
}

JsonValue* json_parse(const char* s) {
    Parser p = {s, 0, strlen(s), 0};
    JsonValue* v = parse_value(&p);
    if (!v) return NULL;
    skip_ws(&p);
    if (p.i != p.n) { json_free(v); return NULL; }
    return v;
}