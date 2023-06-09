using Amazon.DynamoDBv2.Model;
using DynamoDB.EventStore.Tests.Common.TestDomain;

namespace DynamoDB.EventStore.IntegrationTests.Assertion;

internal static class GetItemRequestExtensions
{
    internal static void AssertGetSnapshotItemRequest(this GetItemRequest? request, TestAggregate aggregate, EventStoreConfig config)
    {
        request.Should().NotBeNull();
        request!.TableName.Should().Be(config.TableName);
        request.ConsistentRead.Should().Be(config.ConsistentRead);
        request.Key.Should().ContainKey(config.PartitionKeyName).WhoseValue.S.Should().Be(aggregate.Id);
        request.Key.Should().ContainKey(config.SortKeyName).WhoseValue.N.Should().Be("0");
    }
}