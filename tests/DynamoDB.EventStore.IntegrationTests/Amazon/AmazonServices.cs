using Amazon;
using Amazon.DynamoDBv2;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB;
using DynamoDB.EventStore.IntegrationTests.Amazon.STS;
using DynamoDB.EventStore.Tests.Common.Amazon;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.IntegrationTests.Amazon;

internal sealed class AmazonServices : HttpClientHandler
{
    private readonly string? _listenerName;
    private const string Hostname = "amazonaws.com";
    private const string Region = "us-east-1";

    static AmazonServices()
    {
        AWSConfigs.LoggingConfig.LogTo = LoggingOptions.SystemDiagnostics;
    }

    public AmazonServices(ITestOutputHelper? testOutputHelper = default)
    {
        if (testOutputHelper != default)
        {
            var listener = new TestOutputHelperTraceListener(testOutputHelper);
            _listenerName = listener.Name;
            AWSConfigs.AddTraceListener("Amazon", listener);
        }

        HttpClientFactory = new AmazonHttpClientFactory(this);
    }

    private AmazonHttpClientFactory HttpClientFactory { get; }
    internal AmazonDynamoDBClient CreateDynamoDbClient() => new(
        new ConfigurableAssumeRoleWithWebIdentityCredentials(HttpClientFactory),
        new AmazonDynamoDBConfig
        {
            HttpClientFactory = HttpClientFactory,
            MaxErrorRetry = 0,
            RegionEndpoint = RegionEndpoint.GetBySystemName(Region)
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

    protected override void Dispose(bool disposing)
    {
        if (_listenerName != default)
            AWSConfigs.RemoveTraceListener("Amazon", _listenerName);
    }
}