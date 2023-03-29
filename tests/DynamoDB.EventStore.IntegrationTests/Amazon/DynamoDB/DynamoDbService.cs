using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;
using DynamoDB.EventStore.IntegrationTests.Microsoft.System.Net.Http;

namespace DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB;

internal sealed class DynamoDbService : ObservableHttpClientHandler
{
    internal const string DnsLabel = "dynamodb";

    private const string Version = "20120810";
    internal const string TargetPrefix = $"DynamoDB_{Version}";
    internal readonly string UpdateItemKey = $"{TargetPrefix}.UpdateItem";
    internal readonly string GetItemKey = $"{TargetPrefix}.GetItem";
    internal readonly string QueryKey = $"{TargetPrefix}.Query";

    protected override string MapRequestToHandlerId(HttpRequestMessage message) =>
        $"{message.Headers.GetValues("X-Amz-Target").First()}";

    internal Task<GetItemRequest?> OnGetItemRequest(OnReceiveAsyncHandler handler) => 
        On<GetItemRequest>(GetItemKey, handler);

    internal Task<QueryRequest?> OnQueryRequest(OnReceiveAsyncHandler handler) => 
        On<QueryRequest>(QueryKey, handler);

    private Task<T?> On<T>(string key, OnReceiveAsyncHandler handler)
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

            return await handler(request, cancellationToken)
                .ConfigureAwait(false);
        }
    }

    internal static OnReceiveAsyncHandler ReturnEmptyGetItemResponse = (_, _) => Task.FromResult(new HttpResponseMessage
    {
        Content = new StringContent("""
                {
                    "Attributes": {
                        
                    }
                }
                """)
    });

    internal static OnReceiveAsyncHandler ReturnEmptyQueryResponse = (_, _) => Task.FromResult(new HttpResponseMessage
    {
        Content = new StringContent("""
                {
                    "Attributes": {
                        
                    }
                }
                """)
    });
}