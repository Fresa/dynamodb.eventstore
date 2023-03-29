namespace DynamoDB.EventStore;

public record EventStoreConfig(
    string TableName = "EventStore",
    bool ConsistentRead = false);