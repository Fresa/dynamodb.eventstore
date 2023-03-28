using System.Text.Json;
using DynamoDB.EventStore.IntegrationTests.TestDomain.Commands;
using DynamoDB.EventStore.IntegrationTests.TestDomain.Events;

namespace DynamoDB.EventStore.IntegrationTests.TestDomain;

internal sealed class TestAggregate : Aggregate
{
    public TestAggregate(string id) : base(id)
    {
    }

    public string? Name { get; set; }

    public async Task ChangeNameAsync(ChangeName command, CancellationToken cancellationToken = default)
    {
        var @event = new NameChanged(command.Name);
        await ApplyAsync(@event, cancellationToken)
            .ConfigureAwait(false);
    }

    private void On(NameChanged @event)
    {
        Name = @event.Name;
    }

    protected override async Task<MemoryStream> CreateSnapShotAsync(CancellationToken cancellationToken)
    {
        var dto = new TestSnapshotDto
        {
            Name = Name
        };
        var stream = await WriteAsync(dto, cancellationToken)
            .ConfigureAwait(false);
        return stream;
    }

    protected override async Task LoadSnapshotAsync(MemoryStream snapshot, CancellationToken cancellationToken)
    {
        var dto = await ReadAsync<TestSnapshotDto>(snapshot, cancellationToken)
            .ConfigureAwait(false);
        Name = dto.Name;
    }

    protected override async Task LoadEventsAsync(IEnumerable<MemoryStream> events, CancellationToken cancellationToken)
    {
        foreach (var eventStream in events)
        {
            var @event = await ReadAsync<IEvent>(eventStream, cancellationToken)
                .ConfigureAwait(false);
            Route(@event);
        }
    }

    private void Route(IEvent @event)
    {
        switch (@event)
        {
            case NameChanged namesChanged:
                On(namesChanged);
                break;
            default:
                throw new NotImplementedException($"Event of type {@event.GetType().FullName} is unknown");
        }
    }

    private async Task ApplyAsync<T>(T @event, CancellationToken cancellationToken)
        where T : IEvent
    {
        Route(@event);
        var stream = await WriteAsync(@event, cancellationToken)
            .ConfigureAwait(false);
        UncommittedEvents.Add(stream);
    }

    private async Task<MemoryStream> WriteAsync<T>(T @object, CancellationToken cancellationToken)
    {
        var memoryStream = new MemoryStream();
        await JsonSerializer.SerializeAsync(memoryStream, @object, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return memoryStream;
    }

    private async ValueTask<T> ReadAsync<T>(MemoryStream stream, CancellationToken cancellationToken)
    {
        var result = await JsonSerializer.DeserializeAsync<T>(stream, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        if (result == null)
            throw new InvalidOperationException("Deserialized object was null");
        return result;
    }

    private class TestSnapshotDto
    {
        public string? Name { get; set; }
    }
}