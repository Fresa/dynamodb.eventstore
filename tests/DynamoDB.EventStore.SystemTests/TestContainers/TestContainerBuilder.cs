using Docker.DotNet.Models;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DynamoDB.EventStore.SystemTests.Telemetry;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.SystemTests.TestContainers;

internal sealed class TestContainerBuilder : ContainerBuilder<TestContainerBuilder, IContainer, IContainerConfiguration>
{
    private readonly ITestOutputHelper _testOutputHelper;

    public TestContainerBuilder(ITestOutputHelper testOutputHelper) : 
        this(testOutputHelper, new ContainerConfiguration())
    {
        _testOutputHelper = testOutputHelper;
        DockerResourceConfiguration = Init().DockerResourceConfiguration;
    }

    private TestContainerBuilder(
        ITestOutputHelper testOutputHelper,
        IContainerConfiguration containerConfiguration)
        : base(containerConfiguration)
    {
        _testOutputHelper = testOutputHelper;
        DockerResourceConfiguration = containerConfiguration;
    }

    protected override IContainerConfiguration DockerResourceConfiguration { get; }

    public override IContainer Build()
    {
        Validate();
        return new DockerContainer(DockerResourceConfiguration,
            XUnitLogger.CreateLogger<DockerContainer>(_testOutputHelper));
    }

    protected override TestContainerBuilder Clone(IResourceConfiguration<CreateContainerParameters> resourceConfiguration) => 
        Merge(DockerResourceConfiguration, new ContainerConfiguration(resourceConfiguration));

    protected override TestContainerBuilder Merge(IContainerConfiguration oldValue, IContainerConfiguration newValue) =>
        new(_testOutputHelper,
            new ContainerConfiguration(oldValue, newValue));

    protected override TestContainerBuilder Clone(IContainerConfiguration resourceConfiguration) => 
        Merge(DockerResourceConfiguration, new ContainerConfiguration(resourceConfiguration));
}