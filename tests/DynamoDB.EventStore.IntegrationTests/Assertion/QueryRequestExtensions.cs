using Amazon.DynamoDBv2.Model;
using DynamoDB.EventStore.Tests.Common.TestDomain;

namespace DynamoDB.EventStore.IntegrationTests.Assertion;

internal static class QueryRequestExtensions
{
    internal static void AssertEventsQueried(this QueryRequest? request, TestAggregate aggregate, EventStoreConfig config, int? version = null)
    {
        request.Should().NotBeNull();
        request!.TableName.Should().Be(config.TableName);
        request.ConsistentRead.Should().Be(config.ConsistentRead);
        request.KeyConditionExpression.Should().Be("PK = :PK");
        request.ExpressionAttributeValues.Should().HaveCount(1);
        var payloadExpression = request.ExpressionAttributeValues.Single();
        payloadExpression.Key.Should().Be(":PK");
        payloadExpression.Value.S.Should().Be(aggregate.Id);
        if (version != null)
        {
            request.ExclusiveStartKey.Should().HaveCount(2);
            request.ExclusiveStartKey.Should().ContainKey("PK")
                .WhoseValue.S.Should().Be(aggregate.Id);
            request.ExclusiveStartKey.Should().ContainKey("SK")
                .WhoseValue.S.Should().Be(version.ToString());
        }
        else
        {
            request.ExclusiveStartKey.Should().HaveCount(0);
        }
    }
}