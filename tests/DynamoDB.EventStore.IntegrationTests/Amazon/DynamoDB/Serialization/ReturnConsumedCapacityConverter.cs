using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2;

namespace DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;

internal sealed class ReturnConsumedCapacityConverter : JsonConverter<ReturnConsumedCapacity>
{
    public override ReturnConsumedCapacity? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) => 
        ReturnConsumedCapacity.FindValue(reader.GetString());

    public override void Write(Utf8JsonWriter writer, ReturnConsumedCapacity value, JsonSerializerOptions options)
    {
        writer.WriteString("ReturnConsumedCapacity", value.Value);
    }
}