namespace DynamoDB.EventStore.SystemTests.TestDomain.Events;

internal record NameChanged(string Name) : IEvent;