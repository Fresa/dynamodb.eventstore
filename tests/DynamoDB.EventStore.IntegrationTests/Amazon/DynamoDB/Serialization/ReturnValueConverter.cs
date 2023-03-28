using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2;

namespace DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;

internal sealed class ReturnValueConverter : JsonConverter<ReturnValue>
{
    public override ReturnValue? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return new ReturnValue(reader.GetString());
    }

    public override void Write(Utf8JsonWriter writer, ReturnValue value, JsonSerializerOptions options)
    {
        writer.WriteString("ReturnValues", value.Value);
    }
}