using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DynamoDB.EventStore.SystemTests.Amazon;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.SystemTests;

public abstract class TestSpecification : IAsyncLifetime
{
    private readonly ITestOutputHelper? _testOutputHelper;
    protected CancellationToken TimeoutToken => _timeoutSource.Token;
    protected TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(20);

    private CancellationTokenSource _timeoutSource = default!;

    private const int LocalStackContainerPort = 4566;
    private IContainer _localStack = new ContainerBuilder()
        .WithImage("localstack/localstack:1.4.0")
        .WithPortBinding(LocalStackContainerPort, true)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPath("/_localstack/health").ForPort(LocalStackContainerPort)))
        .Build();

    protected TestSpecification(ITestOutputHelper? testOutputHelper = default)
    {
        _testOutputHelper = testOutputHelper;
    }

    
    internal AmazonServices AmazonServices { get; private set; } = default!;

    public async Task InitializeAsync()
    {
        _timeoutSource = new CancellationTokenSource(Timeout);
        await _localStack.StartAsync(TimeoutToken)
            .ConfigureAwait(false);
        var localStackServiceUrl =
            $"http://{_localStack.Hostname}:{_localStack.GetMappedPublicPort(LocalStackContainerPort)}";
        AmazonServices = new AmazonServices(localStackServiceUrl, _testOutputHelper);
    }

    public async Task DisposeAsync()
    {
        AmazonServices.Dispose();
        await _localStack.DisposeAsync()
            .ConfigureAwait(false);
        _timeoutSource.Dispose();
    }
}