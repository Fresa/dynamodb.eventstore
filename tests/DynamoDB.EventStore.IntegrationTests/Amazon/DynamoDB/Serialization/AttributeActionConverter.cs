using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2;

namespace DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;

internal sealed class AttributeActionConverter : JsonConverter<AttributeAction>
{
    public override AttributeAction? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        return new AttributeAction(reader.GetString());
    }

    public override void Write(Utf8JsonWriter writer, AttributeAction value, JsonSerializerOptions options)
    {
        writer.WriteString("Action", value.Value);
    }
}