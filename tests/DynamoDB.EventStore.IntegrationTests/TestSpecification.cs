using DynamoDB.EventStore.IntegrationTests.Amazon;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.IntegrationTests;

public abstract class EventStoreTestSpecification : IAsyncLifetime
{
    private readonly ITestOutputHelper? _testOutputHelper;
    protected CancellationToken TimeoutToken => _timeoutSource.Token;
    protected TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(3);

    private CancellationTokenSource _timeoutSource = default!;
    
    protected EventStoreTestSpecification(ITestOutputHelper? testOutputHelper = default)
    {
        _testOutputHelper = testOutputHelper;
    }

    internal AmazonServices AmazonServices { get; private set; } = default!;

    public Task InitializeAsync()
    {
        _timeoutSource = new CancellationTokenSource(Timeout);
        AmazonServices = new AmazonServices(_testOutputHelper);
        
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        AmazonServices.Dispose();
        _timeoutSource.Dispose();
        return Task.CompletedTask;
    }
}