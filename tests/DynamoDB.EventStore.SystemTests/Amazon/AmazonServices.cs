using System.Diagnostics;
using Amazon;
using Amazon.DynamoDBv2;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.SystemTests.Amazon;

internal sealed class AmazonServices : IDisposable
{
    private readonly string _serviceUrl;
    private readonly TraceListener? _listener;
    private const string TraceSource = "Amazon";

    static AmazonServices()
    {
        AWSConfigs.LoggingConfig.LogTo = LoggingOptions.SystemDiagnostics;
    }

    public AmazonServices(string serviceUrl, ITestOutputHelper? testOutputHelper = default)
    {
        _serviceUrl = serviceUrl;

        if (testOutputHelper == default)
            return;
        _listener = new TestOutputHelperTraceListener(testOutputHelper);
        AWSConfigs.AddTraceListener(TraceSource, _listener);
    }

    internal AmazonDynamoDBClient CreateDynamoDbClient() =>
        new(
            "test", "test",
            new AmazonDynamoDBConfig
            {
                ServiceURL = _serviceUrl
            });

    public void Dispose()
    {
        if (_listener != default)
            AWSConfigs.RemoveTraceListener(TraceSource, _listener.Name);
    }
}
