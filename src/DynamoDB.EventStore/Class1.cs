using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

namespace DynamoDB.EventStore
{
    public class TestAggregate : Aggregate
    {
        protected override Task<MemoryStream> CreateSnapShotAsync(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task LoadSnapshotAsync(MemoryStream snapshot, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        protected override Task LoadEventsAsync(IEnumerable<MemoryStream> events, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public new string Id => base.Id;

        public TestAggregate(string id) : base(id)
        {
        }
    }

    public abstract class Aggregate
    {
        private const int ReadRequestUnit = 4096;
        private const int WriteRequestUnit = 1024;

        protected Aggregate(string id)
        {
            Id = id;
        }

        protected string Id { get; }
        internal string IdInternal => Id;

        protected int Version => VersionInternal;
        internal int VersionInternal { get; set; }

        protected long BytesSinceLastSnapshot => BytesSinceLastSnapshotInternal;
        internal long BytesSinceLastSnapshotInternal { get; set; }

        protected abstract Task<MemoryStream> CreateSnapShotAsync(CancellationToken cancellationToken);
        internal Task<MemoryStream> CreateSnapShotInternalAsync(CancellationToken cancellationToken = default) => CreateSnapShotAsync(cancellationToken);

        protected bool ShouldCreateSnapshot => BytesSinceLastSnapshot > ReadRequestUnit * 3;
        internal bool ShouldCreateSnapshotInternal => ShouldCreateSnapshot;

        protected virtual bool ShouldReadSnapshots => true;
        internal bool ShouldReadSnapshotsInternal => ShouldReadSnapshots;

        protected abstract Task LoadSnapshotAsync(MemoryStream snapshot, CancellationToken cancellationToken);
        internal Task LoadSnapshotInternalAsync(MemoryStream snapshot, CancellationToken cancellationToken = default) => LoadSnapshotAsync(snapshot, cancellationToken);

        protected abstract Task LoadEventsAsync(IEnumerable<MemoryStream> events, CancellationToken cancellationToken);
        internal Task LoadEventsInternalAsync(IEnumerable<MemoryStream> events, CancellationToken cancellationToken = default) => LoadEventsAsync(events, cancellationToken);

        protected List<MemoryStream> UncommittedEvents { get; } = new();
        internal List<MemoryStream> UncommittedEventsInternal => UncommittedEvents;
    }

    public interface IEventStore
    {
        public Task LoadAsync(Aggregate aggregate,
            CancellationToken cancellationToken = default);
        public Task SaveAsync(Aggregate aggregate,
            CancellationToken cancellationToken = default);
    }

    public record EventStoreConfig(
        string TableName = "EventStore",
        bool ConsistentRead = false);
    
    public class EventStore : IEventStore
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

                    var version = snapshotResponse.Item[TableKeys.Version].S;
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
            aggregate.UncommittedEventsInternal.Clear();

            if (aggregate.ShouldCreateSnapshotInternal)
            {
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
                            B = await aggregate.CreateSnapShotInternalAsync(cancellationToken)
                                    .ConfigureAwait(false),
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
}