using System.Net.Http.Json;
using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;

internal static class HttpContentJsonExtensions
{
    private static JsonSerializerOptions JsonSerializerOptions => new(new JsonSerializerOptions
    {
        Converters =
        {
            new AttributeActionConverter(),
            new ReturnValueConverter()
        }
    });

    internal static Task<T?> ReadDynamodDBRequestFromJsonAsync<T>(this HttpContent content,
        CancellationToken cancellationToken) where T : AmazonDynamoDBRequest =>
        content.ReadFromJsonAsync<T>(cancellationToken: cancellationToken,
            options: JsonSerializerOptions);

    internal static Task<GetItemRequest?> ReadGetItemRequestFromJsonAsync(this HttpContent? content,
        CancellationToken cancellationToken) =>
        content == null
            ? Task.FromResult<GetItemRequest?>(null)
            : content.ReadFromJsonAsync<GetItemRequest>(cancellationToken: cancellationToken,
                options: JsonSerializerOptions);

    internal static Task<QueryRequest?> ReadQueryRequestFromJsonAsync(this HttpContent? content,
        CancellationToken cancellationToken) =>
        content == null
            ? Task.FromResult<QueryRequest?>(null)
            : content.ReadFromJsonAsync<QueryRequest>(cancellationToken: cancellationToken,
                options: JsonSerializerOptions);
}