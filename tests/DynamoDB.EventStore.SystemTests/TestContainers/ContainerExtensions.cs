using DotNet.Testcontainers.Containers;

namespace DynamoDB.EventStore.SystemTests.TestContainers;

internal static class ContainerExtensions
{
    internal static Uri GetUrl(this IContainer container, int port) =>
        new($"http://{container.Hostname}:{container.GetMappedPublicPort(port)}");
}