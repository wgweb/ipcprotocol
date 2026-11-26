// JsonPayload.cs — AOT 安全的 JSON 写入层。
//
// 设计目标：
//  1. 协议自身的固定报文（HELLO / HELLO_ACK / REQUEST / 错误）全部手写 Utf8JsonWriter，
//     不经过 System.Text.Json 的反射序列化，Native AOT 下零元数据依赖；
//  2. 业务载荷优先支持 AOT 可见的类型：null / string / bool / 各数值 / JsonElement / byte[] /
//     可枚举集合 / IDictionary；这些都不需要反射；
//  3. 只有运行时的任意 POCO 才会回退到反射序列化，该分支在 Native AOT
//     （JsonSerializer.IsReflectionEnabledByDefault == false）下会抛出明确的 NotSupportedException，
//     提示调用方改用 JsonElement 或 JsonTypeInfo 重载。

using System.Buffers;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace IpcProtocol;

internal static class JsonPayload
{
    internal static readonly JsonWriterOptions WriterOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,  // 与 JS JSON.stringify 对非 ASCII 的处理对齐
    };

    /// <summary>反射回退路径专用选项，保持与旧版一致的 byte[] 数字数组语义。</summary>
    internal static readonly JsonSerializerOptions ReflectionOptions = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        Converters = { new ByteArrayAsNumberArrayConverter() },
    };

    private sealed class ByteArrayAsNumberArrayConverter : JsonConverter<byte[]>
    {
        public override byte[] Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartArray)
                throw new JsonException("Expected array of numbers for byte[]");
            var list = new List<byte>();
            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                list.Add(reader.GetByte());
            return list.ToArray();
        }

        public override void Write(Utf8JsonWriter writer, byte[] value, JsonSerializerOptions options)
        {
            writer.WriteStartArray();
            foreach (var b in value) writer.WriteNumberValue(b);
            writer.WriteEndArray();
        }
    }

    internal static byte[] Build(Action<Utf8JsonWriter> body)
    {
        var buffer = new ArrayBufferWriter<byte>(256);
        using (var writer = new Utf8JsonWriter(buffer, WriterOptions))
        {
            body(writer);
        }
        return buffer.WrittenSpan.ToArray();
    }

    // ========== 协议固定报文 ==========

    internal static byte[] Error(string message) => Build(w =>
    {
        w.WriteStartObject();
        w.WriteString("error", message);
        w.WriteEndObject();
    });

    internal static byte[] Hello(int version, string? token, string[] capabilities, string impl) => Build(w =>
    {
        w.WriteStartObject();
        w.WriteNumber("version", version);
        if (token is not null) w.WriteString("token", token);
        WriteCaps(w, capabilities);
        w.WriteString("impl", impl);
        w.WriteEndObject();
    });

    internal static byte[] HelloAck(bool ok, int version, string[] capabilities, string impl) => Build(w =>
    {
        w.WriteStartObject();
        w.WriteNumber("version", version);
        w.WriteBoolean("ok", ok);
        WriteCaps(w, capabilities);
        w.WriteString("impl", impl);
        w.WriteEndObject();
    });

    internal static byte[] Request(string method, object? parameters, string? to) => Build(w =>
    {
        w.WriteStartObject();
        w.WriteString("method", method);
        w.WritePropertyName("params");
        WriteValue(w, parameters);
        if (to is not null) w.WriteString("to", to);
        w.WriteEndObject();
    });

    private static void WriteCaps(Utf8JsonWriter w, string[] capabilities)
    {
        w.WriteStartArray("caps");
        foreach (var cap in capabilities) w.WriteStringValue(cap);
        w.WriteEndArray();
    }

    // ========== 通用载荷 ==========

    internal static byte[] Serialize(object? value) => Build(w => WriteValue(w, value));

    /// <summary>写入任意载荷。AOT 可见类型走无反射分支；其余回退反射（AOT 下抛异常）。</summary>
    internal static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                return;
            case string s:
                writer.WriteStringValue(s);
                return;
            case bool b:
                writer.WriteBooleanValue(b);
                return;
            // 数值类型：Utf8JsonWriter 没有 short/sbyte/byte/ushort 重载，统一提升到 int
            case int i:
                writer.WriteNumberValue(i);
                return;
            case long l:
                writer.WriteNumberValue(l);
                return;
            case short s16:
                writer.WriteNumberValue((int)s16);
                return;
            case sbyte sb:
                writer.WriteNumberValue((int)sb);
                return;
            case byte u8:
                writer.WriteNumberValue((int)u8);
                return;
            case ushort u16:
                writer.WriteNumberValue((int)u16);
                return;
            case uint u32:
                writer.WriteNumberValue(u32);
                return;
            case ulong u64:
                writer.WriteNumberValue(u64);
                return;
            case float f:
                writer.WriteNumberValue(f);
                return;
            case double d:
                writer.WriteNumberValue(d);
                return;
            case decimal m:
                writer.WriteNumberValue(m);
                return;
            case JsonElement element:
                element.WriteTo(writer);
                return;
            case byte[] bytes:
                writer.WriteStartArray();
                foreach (var b in bytes) writer.WriteNumberValue(b);
                writer.WriteEndArray();
                return;
            case IDictionary<string, object?> genericDictionary:
                writer.WriteStartObject();
                foreach (var pair in genericDictionary)
                {
                    writer.WritePropertyName(pair.Key);
                    WriteValue(writer, pair.Value);
                }
                writer.WriteEndObject();
                return;
            case IDictionary dictionary:
                writer.WriteStartObject();
                foreach (DictionaryEntry entry in dictionary)
                {
                    writer.WritePropertyName(entry.Key?.ToString() ?? string.Empty);
                    WriteValue(writer, entry.Value);
                }
                writer.WriteEndObject();
                return;
            case IEnumerable enumerable:
                writer.WriteStartArray();
                foreach (var item in enumerable) WriteValue(writer, item);
                writer.WriteEndArray();
                return;
            default:
                WriteReflected(writer, value);
                return;
        }
    }

    /// <summary>
    /// 运行时任意 POCO 的反射回退。仅在 JIT 构建可达；Native AOT 下
    /// JsonSerializer.IsReflectionEnabledByDefault 为 false，会直接抛 NotSupportedException。
    /// 反射调用本身被显式抑制分析器告警（因为已被运行时开关保护）。
    /// </summary>
    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "AOT 下由 IsReflectionEnabledByDefault 运行时开关拦截，永远不可达")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "AOT 下由 IsReflectionEnabledByDefault 运行时开关拦截，永远不可达")]
    private static void WriteReflected(Utf8JsonWriter writer, object value)
    {
        if (!JsonSerializer.IsReflectionEnabledByDefault)
        {
            throw new NotSupportedException(
                $"Native AOT 下无法反射序列化类型 {value.GetType().FullName}；" +
                "请改用 JsonElement / 基础类型，或使用带 JsonTypeInfo<T> 的重载（如 RpcProtocol.Json(value, typeInfo)）。");
        }
        writer.WriteRawValue(JsonSerializer.SerializeToUtf8Bytes(value, value.GetType(), ReflectionOptions));
    }
}
