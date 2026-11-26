// 零依赖 JSON 编解码，语义对齐 JS JSON.stringify/parse。
// Bytes 类型仅用于顶层载荷/参数；参与 JSON 序列化时输出数字数组（对齐 JSON.stringify(Buffer)）。
#pragma once

#include <cstdint>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

namespace ipc {

class Json {
public:
    enum class Type { Null, Bool, Num, Str, Arr, Obj, Bytes };

    Type type = Type::Null;
    bool boolean = false;
    double number = 0.0;
    std::string str;
    std::vector<Json> arr;
    std::map<std::string, Json> obj;
    std::vector<uint8_t> bytes;

    static Json null() { return Json(); }
    static Json ofBool(bool b) { Json j; j.type = Type::Bool; j.boolean = b; return j; }
    static Json ofNum(double n) { Json j; j.type = Type::Num; j.number = n; return j; }
    static Json ofStr(std::string s) { Json j; j.type = Type::Str; j.str = std::move(s); return j; }
    static Json ofArr(std::vector<Json> a) { Json j; j.type = Type::Arr; j.arr = std::move(a); return j; }
    static Json ofBytes(std::vector<uint8_t> b) { Json j; j.type = Type::Bytes; j.bytes = std::move(b); return j; }
    static Json object() { Json j; j.type = Type::Obj; return j; }

    Json& set(const std::string& key, Json v) {
        obj[key] = std::move(v);
        return *this;
    }

    const Json* find(const std::string& key) const {
        auto it = obj.find(key);
        return it == obj.end() ? nullptr : &it->second;
    }

    double asNum() const { return number; }
    const std::string& asStr() const { return str; }
    const std::vector<Json>& asArr() const { return arr; }
    const std::vector<uint8_t>& asBytes() const { return bytes; }

    // JS 真值语义：null/false/0/""/NaN 为假，其余（含空对象/数组）为真。
    bool isTruthy() const {
        switch (type) {
            case Type::Null: return false;
            case Type::Bool: return boolean;
            case Type::Num: return number != 0.0 && !std::isnan(number);
            case Type::Str: return !str.empty();
            default: return true;
        }
    }

    std::string dump() const {
        std::string out;
        writeValue(out);
        return out;
    }

    static Json parse(const std::string& text) {
        Parser p{text, 0};
        p.skipWs();
        Json v = p.value();
        p.skipWs();
        if (p.i != text.size()) throw std::runtime_error("json: trailing characters");
        return v;
    }

private:
    void writeValue(std::string& out) const {
        switch (type) {
            case Type::Null: out += "null"; break;
            case Type::Bool: out += boolean ? "true" : "false"; break;
            case Type::Num: writeNumber(out); break;
            case Type::Str: writeString(out, str); break;
            case Type::Bytes: {
                out += '[';
                for (size_t i = 0; i < bytes.size(); i++) {
                    if (i) out += ',';
                    out += std::to_string(static_cast<int>(bytes[i]));
                }
                out += ']';
                break;
            }
            case Type::Arr: {
                out += '[';
                for (size_t i = 0; i < arr.size(); i++) {
                    if (i) out += ',';
                    arr[i].writeValue(out);
                }
                out += ']';
                break;
            }
            case Type::Obj: {
                out += '{';
                bool first = true;
                for (const auto& [k, v] : obj) {
                    if (!first) out += ',';
                    first = false;
                    writeString(out, k);
                    out += ':';
                    v.writeValue(out);
                }
                out += '}';
                break;
            }
        }
    }

    void writeNumber(std::string& out) const {
        if (std::isnan(number) || std::isinf(number)) {
            out += "null";
        } else if (number == std::floor(number) && std::fabs(number) < 1e15) {
            out += std::to_string(static_cast<long long>(number));
        } else {
            char buf[40];
            snprintf(buf, sizeof(buf), "%.17g", number);
            out += buf;
        }
    }

    static void writeString(std::string& out, const std::string& s) {
        out += '"';
        for (unsigned char c : s) {
            switch (c) {
                case '"': out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\n': out += "\\n"; break;
                case '\r': out += "\\r"; break;
                case '\t': out += "\\t"; break;
                case '\b': out += "\\b"; break;
                case '\f': out += "\\f"; break;
                default:
                    if (c < 0x20) {
                        char buf[8];
                        snprintf(buf, sizeof(buf), "\\u%04x", c);
                        out += buf;
                    } else {
                        out += static_cast<char>(c); // UTF-8 原样输出
                    }
            }
        }
        out += '"';
    }

    struct Parser {
        // 递归深度上限：防止恶意深嵌套 JSON 打爆栈（超限抛异常走解析失败路径）
        static constexpr int MAX_DEPTH = 64;

        const std::string& s;
        size_t i;
        int depth = 0;

        char peek() const { return i < s.size() ? s[i] : '\0'; }

        void skipWs() {
            while (i < s.size() && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r')) i++;
        }

        void expect(char c) {
            if (peek() != c) throw std::runtime_error(std::string("json: expected '") + c + "'");
            i++;
        }

        Json value() {
            char c = peek();
            if (c == '{' || c == '[') {
                if (depth >= MAX_DEPTH) throw std::runtime_error("json: max depth exceeded");
                depth++;
                Json v = (c == '{') ? object() : array();
                depth--;
                return v;
            }
            if (c == '"') return Json::ofStr(string());
            if (c == 't') { literal("true"); return Json::ofBool(true); }
            if (c == 'f') { literal("false"); return Json::ofBool(false); }
            if (c == 'n') { literal("null"); return Json::null(); }
            if (c == '-' || (c >= '0' && c <= '9')) return number();
            throw std::runtime_error("json: unexpected token");
        }

        void literal(const char* text) {
            size_t n = strlen(text);
            if (s.compare(i, n, text) != 0) throw std::runtime_error("json: invalid literal");
            i += n;
        }

        Json object() {
            expect('{');
            Json j = Json::object();
            skipWs();
            if (peek() == '}') { i++; return j; }
            for (;;) {
                skipWs();
                std::string key = string();
                skipWs();
                expect(':');
                skipWs();
                j.set(key, value());
                skipWs();
                if (peek() == ',') { i++; continue; }
                expect('}');
                return j;
            }
        }

        Json array() {
            expect('[');
            std::vector<Json> a;
            skipWs();
            if (peek() == ']') { i++; return Json::ofArr(std::move(a)); }
            for (;;) {
                skipWs();
                a.push_back(value());
                skipWs();
                if (peek() == ',') { i++; continue; }
                expect(']');
                return Json::ofArr(std::move(a));
            }
        }

        std::string string() {
            expect('"');
            std::string out;
            for (;;) {
                if (i >= s.size()) throw std::runtime_error("json: unterminated string");
                unsigned char c = static_cast<unsigned char>(s[i++]);
                if (c == '"') return out;
                if (c != '\\') { out += static_cast<char>(c); continue; }
                if (i >= s.size()) throw std::runtime_error("json: unterminated escape");
                char e = s[i++];
                switch (e) {
                    case '"': out += '"'; break;
                    case '\\': out += '\\'; break;
                    case '/': out += '/'; break;
                    case 'b': out += '\b'; break;
                    case 'f': out += '\f'; break;
                    case 'n': out += '\n'; break;
                    case 'r': out += '\r'; break;
                    case 't': out += '\t'; break;
                    case 'u': {
                        uint32_t hi = hex4();
                        uint32_t cp = hi;
                        if (hi >= 0xD800 && hi <= 0xDBFF) {
                            expect('\\');
                            expect('u');
                            uint32_t lo = hex4();
                            if (lo < 0xDC00 || lo > 0xDFFF) throw std::runtime_error("json: bad surrogate");
                            cp = 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00);
                        }
                        appendUtf8(out, cp);
                        break;
                    }
                    default: throw std::runtime_error("json: invalid escape");
                }
            }
        }

        uint32_t hex4() {
            if (i + 4 > s.size()) throw std::runtime_error("json: truncated \\u");
            uint32_t v = 0;
            for (int k = 0; k < 4; k++) {
                char c = s[i++];
                v <<= 4;
                if (c >= '0' && c <= '9') v |= c - '0';
                else if (c >= 'a' && c <= 'f') v |= c - 'a' + 10;
                else if (c >= 'A' && c <= 'F') v |= c - 'A' + 10;
                else throw std::runtime_error("json: bad hex digit");
            }
            return v;
        }

        static void appendUtf8(std::string& out, uint32_t cp) {
            if (cp < 0x80) {
                out += static_cast<char>(cp);
            } else if (cp < 0x800) {
                out += static_cast<char>(0xC0 | (cp >> 6));
                out += static_cast<char>(0x80 | (cp & 0x3F));
            } else if (cp < 0x10000) {
                out += static_cast<char>(0xE0 | (cp >> 12));
                out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
                out += static_cast<char>(0x80 | (cp & 0x3F));
            } else {
                out += static_cast<char>(0xF0 | (cp >> 18));
                out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
                out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
                out += static_cast<char>(0x80 | (cp & 0x3F));
            }
        }

        Json number() {
            size_t start = i;
            while (i < s.size()) {
                char c = s[i];
                if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E') i++;
                else break;
            }
            return Json::ofNum(std::stod(s.substr(start, i - start)));
        }
    };
};

} // namespace ipc