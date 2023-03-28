namespace DynamoDB.EventStore.IntegrationTests.TestDomain.Events;

internal record NameChanged(string Name) : IEvent;