// 冒烟测试专用的源生成 JSON 上下文：AOT 下所有载荷类型都必须在这里登记。
using System.Text.Json.Serialization;

namespace IpcProtocol.AotSmoke;

public sealed record AddArgs(int A, int B);

public sealed record AddResult(int Sum);

public sealed record UploadResult(int Size, string Tag);

[JsonSourceGenerationOptions(GenerationMode = JsonSourceGenerationMode.Default)]
[JsonSerializable(typeof(AddArgs))]
[JsonSerializable(typeof(AddResult))]
[JsonSerializable(typeof(UploadResult))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(byte[]))]
internal sealed partial class SmokeJson : JsonSerializerContext
{
}
