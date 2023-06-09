using Amazon.DynamoDBv2.Model;
using DynamoDB.EventStore.Tests.Common.TestDomain;

namespace DynamoDB.EventStore.IntegrationTests.Assertion;

internal static class UpdateItemRequestExtensions
{
    internal static void AssertEventsAdded(this UpdateItemRequest? request, TestAggregate aggregate, EventStoreConfig config, byte[][] committedEvents)
    {
        request.Should().NotBeNull();
        request!.TableName.Should().Be(config.TableName);
        request.Key.Should().ContainKey(config.PartitionKeyName).WhoseValue.S.Should().Be(aggregate.Id);
        request.Key.Should().ContainKey(config.SortKeyName).WhoseValue.N.Should().Be(aggregate.Version.ToString());
        request.ConditionExpression.Should().Be($"attribute_not_exists({config.PartitionKeyName})");
        request.UpdateExpression.Should().Be("set P = :P");
        request.ExpressionAttributeValues.Should().HaveCount(1);
        var payloadExpression = request.ExpressionAttributeValues.Single();
        payloadExpression.Key.Should().Be(":P");
        payloadExpression.Value.BS.Should().HaveCount(committedEvents.Length);
        for (var i = 0; i < payloadExpression.Value.BS.Count; i++)
        {
            payloadExpression.Value.BS[i].ToArray().Should().BeEquivalentTo(committedEvents[i].ToArray());
        }
    }

    internal static async Task AssertSnapshotUpdatedAsync(this UpdateItemRequest? request, TestAggregate aggregate,
        EventStoreConfig config, CancellationToken cancellationToken = default)
    {
        request.Should().NotBeNull();
        request!.TableName.Should().Be(config.TableName);
        request.Key.Should().ContainKey(config.PartitionKeyName).WhoseValue.S.Should().Be(aggregate.Id);
        request.Key.Should().ContainKey(config.SortKeyName).WhoseValue.N.Should().Be("0");
        request.ConditionExpression.Should().Be("attribute_not_exists(V) OR V < :V");
        request.UpdateExpression.Should().Be("""
                set 
                    P = :P,
                    V = :V
            """);
        request.ExpressionAttributeValues.Should().HaveCount(2);
        var snapshot = await aggregate.GetSnapShotAsync(cancellationToken)
            .ConfigureAwait(false);
        request.ExpressionAttributeValues.Should().ContainKey(":P")
            .WhoseValue.B.ToArray().Should().BeEquivalentTo(snapshot.ToArray());
        request.ExpressionAttributeValues.Should().ContainKey(":V")
            .WhoseValue.N.Should().Be(aggregate.Version.ToString());
    }
}