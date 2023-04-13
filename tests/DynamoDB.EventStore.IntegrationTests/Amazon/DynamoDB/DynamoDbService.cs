using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;
using DynamoDB.EventStore.IntegrationTests.Microsoft.System.Net.Http;
using DynamoDB.EventStore.IntegrationTests.TestDomain.Events;

namespace DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB;

internal sealed class DynamoDbService : ObservableHttpClientHandler
{
    internal const string DnsLabel = "dynamodb";

    private const string Version = "20120810";
    private const string TargetPrefix = $"DynamoDB_{Version}";
    private const string UpdateItemKey = $"{TargetPrefix}.UpdateItem";
    private const string GetItemKey = $"{TargetPrefix}.GetItem";
    private const string QueryKey = $"{TargetPrefix}.Query";

    protected override string MapRequestToHandlerId(HttpRequestMessage message) =>
        $"{message.Headers.GetValues("X-Amz-Target").First()}";

    internal Task<GetItemRequest?> OnGetItemRequest(OnRequestAsyncHandler handler) =>
        On<GetItemRequest>(GetItemKey, handler);

    internal Task<QueryRequest?> OnQueryRequest(OnRequestAsyncHandler handler) =>
        On<QueryRequest>(QueryKey, handler);

    internal void OnUpdateItemRequest(OnReceiveAsyncHandler handler) =>
        On(UpdateItemKey, handler);

    private Task<T?> On<T>(string key, OnRequestAsyncHandler handler)
        where T : AmazonDynamoDBRequest
    {
        var requestCompletionSource = new TaskCompletionSource<T?>();
        On(key, HandlerWrapper);
        return requestCompletionSource.Task;

        async Task<HttpResponseMessage> HandlerWrapper(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            try
            {
                var amazonDynamoDbRequest = await request.Content.ReadAmazonDynamoDbRequestFromJsonAsync<T>(cancellationToken)
                    .ConfigureAwait(false);
                requestCompletionSource.TrySetResult(amazonDynamoDbRequest);
            }
            catch when (cancellationToken.IsCancellationRequested)
            {
                requestCompletionSource.SetCanceled(cancellationToken);
            }
            catch (Exception e)
            {
                requestCompletionSource.TrySetException(e);
            }

            return await handler(cancellationToken)
                .ConfigureAwait(false);
        }
    }

    internal static OnRequestAsyncHandler ReturnEvents(string aggregateId, params IEvent[][] commits) => _ =>
    {
        var content = $$"""
                {
                    "Count": {{commits.Length}},
                    "Items": [
                        {{string.Join(",", commits.Select((commit, idx) => $$"""
                        {
                            "PK": { "S": "{{aggregateId}}" },
                            "SK": { "S": "{{idx + 1}}" },
                            "P": { "BS": [{{string.Join(", ", commit.Select(@event => $"\"{Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(@event))}\""))}}] }
                        }
                        """))}}
                    ]
                }
                """;
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(content)
            {
                Headers = { ContentType = new MediaTypeHeaderValue("application/x-amz-json-1.0") }
            }
        });
    };

    internal static OnRequestAsyncHandler ReturnSnapshot<T>(string aggregateId, int version, T snapshot) => _ =>
    {
        var content = $$"""
                {
                    "Item": {
                        "PK": { "S": "{{aggregateId}}" },
                        "SK": { "S": "S" },
                        "P": { "B": "{{Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(snapshot))}}" },
                        "V": { "N": "{{version}}" }
                    }
                }
                """;
        return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(content)
            {
                Headers = { ContentType = new MediaTypeHeaderValue("application/x-amz-json-1.0") }
            }
        });
    };

    internal static readonly OnRequestAsyncHandler ReturnEmptyGetItemResponse = _ => Task.FromResult(CreateEmptyResponse());

    internal static OnRequestAsyncHandler ReturnEmptyQueryResponse => ReturnEmptyGetItemResponse;

    internal static OnRequestAsyncHandler ReturnEmptyUpdateItemResponse => ReturnEmptyGetItemResponse;

    internal static HttpResponseMessage CreateEmptyResponse() => new()
    {
        Content = new StringContent("""
                {
                    "Attributes": {
                        
                    }
                }
                """)
    };
}