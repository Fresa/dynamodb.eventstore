using Amazon.DynamoDBv2.Model;

namespace DynamoDB.EventStore.IntegrationTests.TestDomain.Assertion;

internal static class QueryRequestExtensions
{
    internal static void AssertEventsQueried(this QueryRequest request, TestAggregate aggregate, EventStoreConfig config)
    {
        request.TableName.Should().Be(config.TableName);
        request.ConsistentRead.Should().Be(config.ConsistentRead);
        request.KeyConditionExpression.Should().Be("PK = :PK");
        request.ExpressionAttributeValues.Should().HaveCount(1);
        var payloadExpression = request.ExpressionAttributeValues.Single();
        payloadExpression.Key.Should().Be(":PK");
        payloadExpression.Value.S.Should().Be(aggregate.Id);
        request.ExclusiveStartKey.Should().HaveCount(0);
    }
}