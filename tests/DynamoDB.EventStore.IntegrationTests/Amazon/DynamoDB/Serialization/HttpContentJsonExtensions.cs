using System.Net.Http.Json;
using System.Text.Json;
using Amazon.DynamoDBv2;

namespace DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;

internal static class HttpContentJsonExtensions
{
    private static JsonSerializerOptions JsonSerializerOptions => new(new JsonSerializerOptions
    {
        Converters =
        {
            new AttributeActionConverter(),
            new ReturnValueConverter(),
            new MemoryStreamConverter(),
            new ReturnConsumedCapacityConverter()
        }
    });
    
    internal static Task<T?> ReadAmazonDynamoDbRequestFromJsonAsync<T>(this HttpContent? content,
        CancellationToken cancellationToken)
        where T : AmazonDynamoDBRequest =>
        content == null
            ? Task.FromResult<T?>(null)
            : content.ReadFromJsonAsync<T>(cancellationToken: cancellationToken,
                options: JsonSerializerOptions);
}