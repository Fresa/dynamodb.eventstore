using Amazon.DynamoDBv2.Model;

namespace DynamoDB.EventStore.IntegrationTests.TestDomain.Assertion;

internal static class GetItemRequestExtensions
{
    internal static void AssertGetSnapshotItemRequest(this GetItemRequest? request, TestAggregate aggregate, EventStoreConfig config)
    {
        request.Should().NotBeNull();
        request!.TableName.Should().Be(config.TableName);
        request.ConsistentRead.Should().Be(config.ConsistentRead);
        request.Key.Should().ContainKey("PK").WhoseValue.S.Should().Be(aggregate.Id);
        request.Key.Should().ContainKey("SK").WhoseValue.S.Should().Be("S");
    }
}