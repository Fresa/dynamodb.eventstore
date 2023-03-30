using Amazon.DynamoDBv2.Model;

namespace DynamoDB.EventStore.IntegrationTests.TestDomain.Assertion;

internal static class UpdateItemRequestExtensions
{
    internal static void AssertEventsAdded(this UpdateItemRequest request, TestAggregate aggregate, EventStoreConfig config, byte[][] committedEvents)
    {
        request.TableName.Should().Be(config.TableName);
        request.Key.Should().ContainKey("PK").WhoseValue.S.Should().Be(aggregate.Id);
        request.Key.Should().ContainKey("SK").WhoseValue.S.Should().Be(aggregate.Version.ToString());
        request.ConditionExpression.Should().Be("attribute_not_exists(PK)");
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

    internal static async Task AssertSnapshotUpdatedAsync(this UpdateItemRequest request, TestAggregate aggregate,
        EventStoreConfig config, CancellationToken cancellationToken = default)
    {
        request.TableName.Should().Be(config.TableName);
        request.Key.Should().ContainKey("PK").WhoseValue.S.Should().Be(aggregate.Id);
        request.Key.Should().ContainKey("SK").WhoseValue.S.Should().Be("S");
        request.ConditionExpression.Should().Be("V < :V");
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