using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDB.EventStore.Tests.Common.TestDomain;
using FluentAssertions;

namespace DynamoDB.EventStore.SystemTests.Amazon;

internal static class DynamoDbClientExtensions
{
    internal static async Task CreateEventStoreTableAsync(this AmazonDynamoDBClient client, EventStoreConfig eventStoreConfig, CancellationToken cancellationToken = default)
    {
        var request = new CreateTableRequest
        {
            TableName = eventStoreConfig.TableName,
            KeySchema = new List<KeySchemaElement>
            {
                new("PK", KeyType.HASH),
                new("SK", KeyType.RANGE)
            },
            AttributeDefinitions = new List<AttributeDefinition>()
            {
                new("PK", ScalarAttributeType.S),
                new("SK", ScalarAttributeType.S)
            },
            ProvisionedThroughput = new ProvisionedThroughput(1, 1)
        };
        await client.CreateTableAsync(request, cancellationToken)
            .ConfigureAwait(false);
    }

    internal static async Task AssertEventsAddedAsync(this AmazonDynamoDBClient client, 
        TestAggregate aggregate, 
        EventStoreConfig config, 
        byte[][] committedEvents, 
        CancellationToken cancellationToken = default)
    {
        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = config.TableName,
            ConsistentRead = config.ConsistentRead,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new(aggregate.Id),
                ["SK"] = new(aggregate.Version.ToString())
            }
        }, cancellationToken);
        
        var payload = response.Item["P"];
        payload.BS.Should().HaveCount(committedEvents.Length);
        for (var i = 0; i < response.Item["P"].BS.Count; i++)
        {
            payload.BS[i].ToArray().Should().BeEquivalentTo(committedEvents[i].ToArray());
        }
    }

    internal static async Task AssertSnapshotUpdatedAsync(this AmazonDynamoDBClient client, 
        TestAggregate aggregate,
        EventStoreConfig config, 
        CancellationToken cancellationToken = default)
    {
        var response = await client.GetItemAsync(new GetItemRequest
        {
            TableName = config.TableName,
            ConsistentRead = config.ConsistentRead,
            Key = new Dictionary<string, AttributeValue>
            {
                ["PK"] = new(aggregate.Id),
                ["SK"] = new("S")
            }
        }, cancellationToken);
        
        var payload = response.Item["P"].B;
        var version = response.Item["V"].N;
        var snapshot = await aggregate.GetSnapShotAsync(cancellationToken)
            .ConfigureAwait(false);

        payload.ToArray().Should().BeEquivalentTo(snapshot.ToArray());
        version.Should().Be(aggregate.Version.ToString());
    }
}