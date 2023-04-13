using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDB.EventStore;

public sealed class EventStore
{
    private readonly IAmazonDynamoDB _client;
    private readonly EventStoreConfig _config;

    public EventStore(IAmazonDynamoDB client, EventStoreConfig config)
    {
        _client = client;
        _config = config;
    }

    private static class TableKeys
    {
        internal const string PartitionKey = "PK";
        internal const string SortKey = "SK";
        internal const string Payload = "P";
        internal const string Version = "V";
    }

    private const string SnapshotItemType = "S";

    public async Task LoadAsync(
        Aggregate aggregate,
        CancellationToken cancellationToken = default)
    {
        var exclusiveStartKey = new Dictionary<string, AttributeValue>();
        if (aggregate.ShouldReadSnapshotsInternal)
        {
            var snapshotResponse = await _client.GetItemAsync(new GetItemRequest
            {
                TableName = _config.TableName,
                ConsistentRead = _config.ConsistentRead,
                Key = new Dictionary<string, AttributeValue>
                {
                    [TableKeys.PartitionKey] = new() { S = aggregate.IdInternal },
                    [TableKeys.SortKey] = new() { S = SnapshotItemType }
                }
            }, cancellationToken)
                .ConfigureAwait(false);

            var hasSnapshot = snapshotResponse.IsItemSet;

            if (hasSnapshot)
            {
                var snapshot = snapshotResponse.Item[TableKeys.Payload].B;
                await aggregate.LoadSnapshotInternalAsync(snapshot, cancellationToken)
                    .ConfigureAwait(false);

                var version = snapshotResponse.Item[TableKeys.Version].N;
                aggregate.VersionInternal = int.Parse(version);

                exclusiveStartKey[TableKeys.PartitionKey] = new AttributeValue { S = aggregate.IdInternal };
                exclusiveStartKey[TableKeys.SortKey] = new AttributeValue { S = version };
            }
        }

        while (true)
        {
            var response = await _client.QueryAsync(new QueryRequest
            {
                TableName = _config.TableName,
                ConsistentRead = _config.ConsistentRead,
                KeyConditionExpression = $"{TableKeys.PartitionKey} = :{TableKeys.PartitionKey}",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [$":{TableKeys.PartitionKey}"] = new() { S = aggregate.IdInternal }
                },
                ExclusiveStartKey = exclusiveStartKey
            }, cancellationToken)
                .ConfigureAwait(false);

            foreach (var payload in response.Items.Select(item => item[TableKeys.Payload]))
            {
                await aggregate.LoadEventsInternalAsync(payload.BS, cancellationToken)
                    .ConfigureAwait(false);
                aggregate.VersionInternal++;
                aggregate.BytesSinceLastSnapshotInternal += payload.BS.Aggregate((long)0, (length, stream) => length + stream.Length);
            }

            if (!response.LastEvaluatedKey.Any())
            {
                return;
            }

            exclusiveStartKey = response.LastEvaluatedKey;
        }
    }

    public async Task SaveAsync(Aggregate aggregate,
        CancellationToken cancellationToken = default)
    {
        if (!aggregate.UncommittedEventsInternal.Any())
            return;

        const string commitConditionExpression = $"attribute_not_exists({TableKeys.PartitionKey})";
        aggregate.VersionInternal++;

        await _client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = _config.TableName,
            Key = new Dictionary<string, AttributeValue>
            {
                [TableKeys.PartitionKey] = new() { S = aggregate.IdInternal },
                [TableKeys.SortKey] = new() { S = $"{aggregate.VersionInternal}" }
            },
            ConditionExpression = commitConditionExpression,
            UpdateExpression = $"set {TableKeys.Payload} = :{TableKeys.Payload}",
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [$":{TableKeys.Payload}"] = new()
                {
                    BS = aggregate.UncommittedEventsInternal
                }
            }
        }, cancellationToken)
            .ConfigureAwait(false);
        
        await Task.WhenAll(
                aggregate.UncommittedEventsInternal
                    .Select(stream => stream.DisposeAsync())
                    .Select(task => task.IsCompletedSuccessfully ? Task.CompletedTask : task.AsTask()))
            .ConfigureAwait(false);
        aggregate.UncommittedEventsInternal.Clear();

        if (aggregate.ShouldCreateSnapshotInternal)
        {
            using var snapshot = await aggregate.CreateSnapShotInternalAsync(cancellationToken)
                .ConfigureAwait(false);
            await _client.UpdateItemAsync(new UpdateItemRequest
            {
                TableName = _config.TableName,
                Key = new Dictionary<string, AttributeValue>
                {
                    [TableKeys.PartitionKey] = new() { S = aggregate.IdInternal },
                    [TableKeys.SortKey] = new() { S = SnapshotItemType }
                },
                ConditionExpression = $"{TableKeys.Version} < :{TableKeys.Version}",
                UpdateExpression = $"""
                        set 
                            {TableKeys.Payload} = :{TableKeys.Payload},
                            {TableKeys.Version} = :{TableKeys.Version}
                    """,
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [$":{TableKeys.Payload}"] = new()
                    {
                        B = snapshot,
                    },
                    [$":{TableKeys.Version}"] = new()
                    {
                        N = $"{aggregate.VersionInternal}"
                    }
                }
            }, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}