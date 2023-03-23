using System.Data;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using System.Threading;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace DynamoDB.EventStore
{
    internal record Event(int Id, byte[] Payload);

    public interface IEvent
    {
        public int Id { get; }
        public MemoryStream Serialize();
    }

    internal record Commit(string AggregateId, int Id, List<byte[]> Events);
    internal record SnapShot(string AggregateId, byte[] Payload);

    public interface ISnapShot
    {
        public string AggregateId { get; }
        public MemoryStream Serialize();
    }

    public interface IAggregate
    {
        public string Id { get; }
        public int Version { get; set; }
        public int LastSnapshotVersion { get; set; }
        public MemoryStream CreateSnapShot();
        public IEnumerable<IEvent> GetUncommittedEvents();
    }


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

    public class EventStore : IEventStore
    {
        private readonly IAmazonDynamoDB _client;

        public EventStore(IAmazonDynamoDB client)
        {
            _client = client;
        }

        private static class TableKeys
        {
            internal const string Id = "I";
            internal const string SortKey = "S";
            internal const string Payload = "P";
            internal const string Version = "V";
        }

        private const string KeyConditionExpression = $"{TableKeys.Id} = :{TableKeys.Id}";
        private const string SnapshotItemType = "S";
        private const string TableName = "EventStore";

        public async Task LoadAsync(
            Aggregate aggregate,
            CancellationToken cancellationToken = default)
        {
            var events = new List<MemoryStream>();
            var exclusiveStartKey = new Dictionary<string, AttributeValue>();
            while (true)
            {
                var response = await _client.QueryAsync(new QueryRequest
                {
                    TableName = TableName,
                    KeyConditionExpression = KeyConditionExpression,
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        [$":{TableKeys.Id}"] = new() { S = aggregate.IdInternal }
                    },
                    ExclusiveStartKey = exclusiveStartKey,
                    ScanIndexForward = false,
                    Limit = 10
                }, cancellationToken)
                    .ConfigureAwait(false);
                
                foreach (var item in response.Items)
                {
                    var type = item[TableKeys.SortKey].S;
                    if (aggregate.VersionInternal == 0)
                    {
                        aggregate.VersionInternal = int.Parse(type.Split('#', 1).First());
                    } 

                    var payload = item[TableKeys.Payload];
                    if (type.EndsWith($"#{SnapshotItemType}"))
                    {
                        var snapshot = payload.B ?? throw new InvalidOperationException("Snapshot payload was empty");
                        await aggregate.LoadSnapshotInternalAsync(snapshot, cancellationToken)
                            .ConfigureAwait(false);
                        await aggregate.LoadEventsInternalAsync(events, cancellationToken)
                            .ConfigureAwait(false);
                        return;
                    }

                    events.AddRange(payload.BS);
                    aggregate.BytesSinceLastSnapshotInternal += payload.BS.Aggregate((long)0, (length, stream) => length + stream.Length);
                }

                if (!response.LastEvaluatedKey.Any())
                {
                    await aggregate.LoadEventsInternalAsync(events, cancellationToken)
                        .ConfigureAwait(false);
                    return;
                }

                exclusiveStartKey = response.LastEvaluatedKey;
            }
        }

        private async Task LoadSeparatelyAsync(Aggregate aggregate,
            CancellationToken cancellationToken = default)
        {
            var exclusiveStartKey = new Dictionary<string, AttributeValue>();
            if (aggregate.ShouldReadSnapshotsInternal)
            {
                var snapshotResponse = await _client.GetItemAsync(new GetItemRequest
                    {
                        TableName = TableName,
                        Key = new Dictionary<string, AttributeValue>
                        {
                            [TableKeys.Id] = new() { S = aggregate.IdInternal },
                            [TableKeys.SortKey] = new() { S = SnapshotItemType }
                        }
                    }, cancellationToken)
                    .ConfigureAwait(false);
                var hasSnapshot = snapshotResponse.Item.Any();

                if (hasSnapshot)
                {
                    var snapshot = snapshotResponse.Item[TableKeys.Payload].B;
                    await aggregate.LoadSnapshotInternalAsync(snapshot, cancellationToken)
                        .ConfigureAwait(false);

                    var version = snapshotResponse.Item[TableKeys.Version].S;
                    aggregate.VersionInternal = int.Parse(version);

                    exclusiveStartKey[TableKeys.Id] = new AttributeValue { S = aggregate.IdInternal };
                    exclusiveStartKey[TableKeys.SortKey] = new AttributeValue { S = version };
                }
            }

            while (true)
            {
                var response = await _client.QueryAsync(new QueryRequest
                    {
                        TableName = TableName,
                        KeyConditionExpression = KeyConditionExpression,
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            [$":{TableKeys.Id}"] = new() { S = aggregate.IdInternal }
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

            const string commitConditionExpression = $"attribute_not_exists({TableKeys.Id})";
            aggregate.VersionInternal++;
            
            await _client.UpdateItemAsync(new UpdateItemRequest
                {
                    Key = new Dictionary<string, AttributeValue>
                    {
                        [TableKeys.Id] = new() { S = aggregate.IdInternal },
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
                // LoadAsync example
                //await _client.UpdateItemAsync(new UpdateItemRequest
                //{
                //    Key = new Dictionary<string, AttributeValue>
                //    {
                //        [TableKeys.Id] = new() { S = aggregate.Id },
                //        [TableKeys.SortKey] = new() { S = $"{aggregate.Version}#{SnapshotItemType}" }
                //    },
                //    ConditionExpression = commitConditionExpression,
                //    UpdateExpression = $"set {TableKeys.Payload} = :payload",
                //    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                //    {
                //        [":payload"] = new()
                //        {
                //            B = aggregate.CreateSnapShot(),
                //        }
                //    }
                //}, cancellationToken).ConfigureAwait(false);

                await _client.UpdateItemAsync(new UpdateItemRequest
                    {
                        Key = new Dictionary<string, AttributeValue>
                        {
                            [TableKeys.Id] = new() { S = aggregate.IdInternal },
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