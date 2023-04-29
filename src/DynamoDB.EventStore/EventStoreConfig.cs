namespace DynamoDB.EventStore;

/// <summary>
/// Event store configuration 
/// </summary>
/// <param name="TableName">The event store table name. Defaults to "EventStore"</param>
/// <param name="PartitionKeyName">The name of the partition key. Defaults to "PK"</param>
/// <param name="SortKeyName">The name of the sort key. Defaults to "SK"</param>
/// <param name="ConsistentRead">If fetching data from DynamoDB should be strongly consistent or not, see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html. Default to false</param>
public record EventStoreConfig(
    string TableName = "EventStore",
    string PartitionKeyName = "PK",
    string SortKeyName = "SK",
    bool ConsistentRead = false);