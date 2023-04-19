namespace DynamoDB.EventStore.Tests.Common.TestDomain.Events;

public record NameChanged(string Name) : IEvent;