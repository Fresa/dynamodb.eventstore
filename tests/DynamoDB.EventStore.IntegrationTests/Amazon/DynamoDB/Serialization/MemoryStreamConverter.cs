using System.Text.Json;
using System.Text.Json.Serialization;

namespace DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;

internal sealed class MemoryStreamConverter : JsonConverter<MemoryStream>
{
    public override MemoryStream Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return new MemoryStream(reader.GetBytesFromBase64(), false);
    }

    public override void Write(Utf8JsonWriter writer, MemoryStream value, JsonSerializerOptions options)
    {
        writer.WriteRawValue(new ReadOnlySpan<byte>(value.GetBuffer())[..(int)value.Length]);
    }
}