using Amazon.DynamoDBv2;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB;
using DynamoDB.EventStore.IntegrationTests.Amazon.STS;

namespace DynamoDB.EventStore.IntegrationTests.Amazon;

internal sealed class AmazonServices : HttpClientHandler
{
    internal const string Hostname = "amazonaws.com";
    internal const string Region = "us-east-1";

    public AmazonServices()
    {
        HttpClientFactory = new AmazonHttpClientFactory(this);
    }

    internal AmazonHttpClientFactory HttpClientFactory { get; } 
    internal AmazonDynamoDBClient CreateDynamoDbClient() => new(
        new ConfigurableAssumeRoleWithWebIdentityCredentials(HttpClientFactory),
        new AmazonDynamoDBConfig
        {
            HttpClientFactory = HttpClientFactory
        });

    internal DynamoDbService DynamoDb { get; } = new();
    internal StsService StsService { get; } = new();

    private const string DynamoDbHostname = $"{DynamoDbService.DnsLabel}.{Region}.{Hostname}";
    private const string StsHostname = $"{StsService.DnsLabel}.{Hostname}";

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        request.RequestUri?.Host switch
        {
            DynamoDbHostname => DynamoDb.SendAsync(request, cancellationToken),
            StsHostname => StsService.SendAsync(request, cancellationToken),
            _ => throw new InvalidOperationException($"{request.RequestUri?.Host} is an unknown Amazon service")
        };
}