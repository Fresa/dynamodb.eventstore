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
}