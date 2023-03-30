using Amazon.DynamoDBv2.Model;

namespace DynamoDB.EventStore.IntegrationTests.TestDomain.Assertion;

internal static class UpdateItemRequestExtensions
{
    internal static void AssertEventsAdded(this UpdateItemRequest updateItemRequest, TestAggregate aggregate, EventStoreConfig config, byte[][] committedEvents)
    {
        updateItemRequest.TableName.Should().Be(config.TableName);
        updateItemRequest.Key.Should().ContainKey("PK").WhoseValue.S.Should().Be(aggregate.Id);
        updateItemRequest.Key.Should().ContainKey("SK").WhoseValue.S.Should().Be(aggregate.Version.ToString());
        updateItemRequest.ConditionExpression.Should().Be("attribute_not_exists(PK)");
        updateItemRequest.UpdateExpression.Should().Be("set P = :P");
        updateItemRequest.ExpressionAttributeValues.Should().HaveCount(1);
        var payloadExpression = updateItemRequest.ExpressionAttributeValues.Single();
        payloadExpression.Key.Should().Be(":P");
        payloadExpression.Value.BS.Should().HaveCount(committedEvents.Length);
        for (var i = 0; i < payloadExpression.Value.BS.Count; i++)
        {
            payloadExpression.Value.BS[i].ToArray().Should().BeEquivalentTo(committedEvents[i].ToArray());
        }
    }
}