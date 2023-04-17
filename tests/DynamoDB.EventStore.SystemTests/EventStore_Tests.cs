using DynamoDB.EventStore.SystemTests.Amazon;
using DynamoDB.EventStore.SystemTests.TestDomain;
using DynamoDB.EventStore.SystemTests.TestDomain.Commands;
using FluentAssertions;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.SystemTests;

public class EventStore_Tests : TestSpecification
{
    public EventStore_Tests(ITestOutputHelper testOutputHelper) : base(testOutputHelper) { }

    [Fact]
    public async Task Given_no_events_stored_When_loading_an_aggregate_It_should_load_the_aggregate()
    {
        using var client = AmazonServices.CreateDynamoDbClient();
        var config = new EventStoreConfig();
        var eventStore = new EventStore(client, config);
        await client.CreateEventStoreTableAsync(config, TimeoutToken)
            .ConfigureAwait(false);

        var aggregate = new TestAggregate("Test");
        await eventStore.LoadAsync(aggregate)
            .ConfigureAwait(false);

        aggregate.Name.Should().BeNull();
    }

    [Fact]
    public async Task Given_no_events_stored_When_saving_events_The_events_should_be_stored()
    {
        using var client = AmazonServices.CreateDynamoDbClient();
        var config = new EventStoreConfig();
        var eventStore = new EventStore(client, config);
        await client.CreateEventStoreTableAsync(config, TimeoutToken)
            .ConfigureAwait(false);

        var aggregate = new TestAggregate("Test", shouldCreateSnapshot: true);
        await aggregate.ChangeNameAsync(new ChangeName("test"))
            .ConfigureAwait(false);

        var uncommittedEvents = aggregate.UncommittedEvents.Select(stream => stream.ToArray()).ToArray();

        await eventStore.SaveAsync(aggregate)
            .ConfigureAwait(false);

        await client.AssertEventsAddedAsync(aggregate, config, uncommittedEvents, TimeoutToken)
            .ConfigureAwait(false);
        await client.AssertSnapshotUpdatedAsync(aggregate, config)
            .ConfigureAwait(false);
    }

    [Fact]
    public async Task Given_events_and_snapshot_When_loading_an_aggregate_It_should_read_the_events()
    {
        using var client = AmazonServices.CreateDynamoDbClient();
        var config = new EventStoreConfig();
        var eventStore = new EventStore(client, config);
        await client.CreateEventStoreTableAsync(config, TimeoutToken)
            .ConfigureAwait(false);

        var aggregate = new TestAggregate("Test", shouldCreateSnapshot: true);
        await aggregate.ChangeNameAsync(new ChangeName("This is a name"), TimeoutToken)
            .ConfigureAwait(false);
        await eventStore.SaveAsync(aggregate)
            .ConfigureAwait(false);

        await aggregate.ChangeNameAsync(new ChangeName("Name2"), TimeoutToken)
            .ConfigureAwait(false);
        aggregate.CreateSnapshot = false;
        await eventStore.SaveAsync(aggregate)
            .ConfigureAwait(false);

        aggregate = new TestAggregate("Test");
        await eventStore.LoadAsync(aggregate, TimeoutToken)
            .ConfigureAwait(false);
        
        aggregate.Name.Should().Be("Name2");
    }
}
