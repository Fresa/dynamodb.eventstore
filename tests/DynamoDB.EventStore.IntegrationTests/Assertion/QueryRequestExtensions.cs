using Amazon.DynamoDBv2;
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
        request.ReturnConsumedCapacity.Should().Be(ReturnConsumedCapacity.TOTAL);
        request.KeyConditionExpression.Should().Be($"{config.PartitionKeyName} = :{config.PartitionKeyName}");
        request.ExpressionAttributeValues.Should().HaveCount(1);
        var payloadExpression = request.ExpressionAttributeValues.Single();
        payloadExpression.Key.Should().Be($":{config.PartitionKeyName}");
        payloadExpression.Value.S.Should().Be(aggregate.Id);
        if (version != null)
        {
            request.ExclusiveStartKey.Should().HaveCount(2);
            request.ExclusiveStartKey.Should().ContainKey(config.PartitionKeyName)
                .WhoseValue.S.Should().Be(aggregate.Id);
            request.ExclusiveStartKey.Should().ContainKey(config.SortKeyName)
                .WhoseValue.N.Should().Be(version.ToString());
        }
        else
        {
            request.ExclusiveStartKey.Should().HaveCount(0);
        }
    }
}