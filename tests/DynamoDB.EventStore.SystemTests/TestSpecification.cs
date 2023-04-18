using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DynamoDB.EventStore.SystemTests.Amazon;
using DynamoDB.EventStore.SystemTests.TestContainers;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.SystemTests;

public abstract class TestSpecification : IAsyncLifetime
{
    protected CancellationToken TimeoutToken => _timeoutSource.Token;
    protected TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(20);

    private CancellationTokenSource _timeoutSource = default!;

    private const int LocalStackContainerPort = 4566;
    private readonly IContainer _localStack;

    protected TestSpecification(ITestOutputHelper testOutputHelper)
    {
        _localStack = new TestContainerBuilder(testOutputHelper)
            .WithImage("localstack/localstack:1.4.0")
            .WithPortBinding(LocalStackContainerPort, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request =>
                request.ForPath("/_localstack/health").ForPort(LocalStackContainerPort)))
            .Build();
        _amazonServicesFactory = new Lazy<AmazonServices>(() =>
            new AmazonServices(_localStack.GetUrl(LocalStackContainerPort).ToString(), testOutputHelper));
    }

    private readonly Lazy<AmazonServices> _amazonServicesFactory;
    internal AmazonServices AmazonServices => _amazonServicesFactory.Value;

    public async Task InitializeAsync()
    {
        _timeoutSource = new CancellationTokenSource(Timeout);
        await _localStack.StartAsync(TimeoutToken)
            .ConfigureAwait(false);
    }

    public async Task DisposeAsync()
    {
        AmazonServices.Dispose();
        await _localStack.DisposeAsync()
            .ConfigureAwait(false);
        _timeoutSource.Dispose();
    }
}