using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDB.EventStore;

public sealed class EventStore
{
    private readonly IAmazonDynamoDB _client;
    private readonly EventStoreConfig _config;

    /// <summary>
    /// Constructs an event store instance
    /// </summary>
    /// <param name="client">A DynamoDB client</param>
    /// <param name="config">Event store configuration</param>
    public EventStore(IAmazonDynamoDB client, EventStoreConfig config)
    {
        _client = client;
        _config = config;
    }

    private static class TableKeys
    {
        internal const string Payload = "P";
        internal const string Version = "V";
    }

    private const string SnapshotItemSortKey = "0";

    /// <summary>
    /// Loads the aggregate's events and snapshot
    /// </summary>
    /// <param name="aggregate">The aggregate to load</param>
    /// <param name="cancellationToken">Cancellation</param>
    /// <exception>See <see cref="IAmazonDynamoDB.GetItemAsync"/> and <see cref="IAmazonDynamoDB.QueryAsync"/></exception>
    /// <returns>An awaitable task</returns>
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
                    [_config.PartitionKeyName] = new() { S = aggregate.IdInternal },
                    [_config.SortKeyName] = new() { N = SnapshotItemSortKey }
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

                exclusiveStartKey[_config.PartitionKeyName] = new AttributeValue { S = aggregate.IdInternal };
                exclusiveStartKey[_config.SortKeyName] = new AttributeValue { N = version };
            }
        }

        aggregate.ReadCapacityUnitsSinceLastSnapshotInternal = 0;
        while (true)
        {
            var response = await _client.QueryAsync(new QueryRequest
            {
                TableName = _config.TableName,
                ConsistentRead = _config.ConsistentRead,
                KeyConditionExpression = $"{_config.PartitionKeyName} = :{_config.PartitionKeyName}",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    [$":{_config.PartitionKeyName}"] = new() { S = aggregate.IdInternal }
                },
                ExclusiveStartKey = exclusiveStartKey,
                ReturnConsumedCapacity = ReturnConsumedCapacity.TOTAL
            }, cancellationToken)
                .ConfigureAwait(false);
            
            aggregate.ReadCapacityUnitsSinceLastSnapshotInternal += response.ConsumedCapacity.CapacityUnits;
            foreach (var payload in response.Items.Select(item => item[TableKeys.Payload]))
            {
                await aggregate.LoadEventsInternalAsync(payload.BS, cancellationToken)
                    .ConfigureAwait(false);
                aggregate.VersionInternal++;
            }

            if (!response.LastEvaluatedKey.Any())
            {
                return;
            }

            exclusiveStartKey = response.LastEvaluatedKey;
        }
    }

    /// <summary>
    /// Saves the <see cref="Aggregate.UncommittedEvents">uncommitted events</see> of an aggregate and writes a snapshot if requested, see <see cref="Aggregate.ShouldCreateSnapshot"/>
    /// </summary>
    /// <param name="aggregate">The aggregate to be saved</param>
    /// <param name="cancellationToken">Cancellation</param>
    /// <exception>See <see cref="IAmazonDynamoDB.UpdateItemAsync"/></exception>
    /// <returns>An awaitable task</returns>
    public async Task SaveAsync(Aggregate aggregate,
        CancellationToken cancellationToken = default)
    {
        if (!aggregate.UncommittedEventsInternal.Any())
            return;

        var commitConditionExpression = $"attribute_not_exists({_config.PartitionKeyName})";
        aggregate.VersionInternal++;

        await _client.UpdateItemAsync(new UpdateItemRequest
        {
            TableName = _config.TableName,
            Key = new Dictionary<string, AttributeValue>
            {
                [_config.PartitionKeyName] = new() { S = aggregate.IdInternal },
                [_config.SortKeyName] = new() { N = $"{aggregate.VersionInternal}" }
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
                    [_config.PartitionKeyName] = new() { S = aggregate.IdInternal },
                    [_config.SortKeyName] = new() { N = SnapshotItemSortKey }
                },
                ConditionExpression = $"attribute_not_exists({TableKeys.Version}) OR {TableKeys.Version} < :{TableKeys.Version}",
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
            aggregate.ReadCapacityUnitsSinceLastSnapshotInternal = 0;
        }
    }
}