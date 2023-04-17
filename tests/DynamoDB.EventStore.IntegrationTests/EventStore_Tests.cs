using Amazon.DynamoDBv2.Model;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;
using DynamoDB.EventStore.IntegrationTests.TestDomain;
using DynamoDB.EventStore.IntegrationTests.TestDomain.Assertion;
using DynamoDB.EventStore.IntegrationTests.TestDomain.Commands;
using DynamoDB.EventStore.IntegrationTests.TestDomain.Events;
using Xunit.Abstractions;

namespace DynamoDB.EventStore.IntegrationTests;

public class EventStore_Tests : TestSpecification
{
    public EventStore_Tests(ITestOutputHelper testOutputHelper) : base(testOutputHelper) { }

    [Fact]
    public async Task Given_no_events_stored_When_loading_an_aggregate_It_should_load_the_aggregate()
    {
        AmazonServices.StsService.RespondWithDefaultAssumeRoleWithWebIdentityResponse();
        var snapshotRequestSubscription = AmazonServices.DynamoDb.OnGetItemRequest(DynamoDbService.ReturnEmptyGetItemResponse);
        var queryEventsRequestSubscription = AmazonServices.DynamoDb.OnQueryRequest(DynamoDbService.ReturnEmptyQueryResponse);

        using var client = AmazonServices.CreateDynamoDbClient();
        var config = new EventStoreConfig();
        var eventStore = new EventStore(client, config);

        var aggregate = new TestAggregate("Test");
        await eventStore.LoadAsync(aggregate)
            .ConfigureAwait(false);

        var snapshotRequest = await snapshotRequestSubscription.ConfigureAwait(false);
        snapshotRequest.AssertGetSnapshotItemRequest(aggregate, config);

        var queryEventsRequest = await queryEventsRequestSubscription.ConfigureAwait(false);
        queryEventsRequest.AssertEventsQueried(aggregate, config);

        aggregate.Name.Should().BeNull();
    }

    [Fact]
    public async Task Given_no_events_stored_When_saving_events_The_events_should_be_stored()
    {
        AmazonServices.StsService.RespondWithDefaultAssumeRoleWithWebIdentityResponse();

        List<UpdateItemRequest?> updateRequests = new();
        AmazonServices.DynamoDb.OnUpdateItemRequest(async (request, cancellation) =>
        {
            var updateItemRequest = await request.Content
                .ReadAmazonDynamoDbRequestFromJsonAsync<UpdateItemRequest>(cancellation)
                .ConfigureAwait(false);
            updateRequests.Add(updateItemRequest);

            return DynamoDbService.CreateEmptyResponse();
        });

        using var client = AmazonServices.CreateDynamoDbClient();
        var config = new EventStoreConfig();
        var eventStore = new EventStore(client, config);

        var aggregate = new TestAggregate("Test", shouldCreateSnapshot: true);
        await aggregate.ChangeNameAsync(new ChangeName("test"))
            .ConfigureAwait(false);

        var uncommittedEvents = aggregate.UncommittedEvents.Select(stream => stream.ToArray()).ToArray();

        await eventStore.SaveAsync(aggregate)
            .ConfigureAwait(false);

        updateRequests.Should().HaveCount(2);
        var eventsAdded = updateRequests.First();
        eventsAdded.AssertEventsAdded(aggregate, config, uncommittedEvents);

        var updateSnapshotRequest = updateRequests[1];
        await updateSnapshotRequest.AssertSnapshotUpdatedAsync(aggregate, config)
            .ConfigureAwait(false);
    }

    [Fact]
    public async Task Given_events_and_snapshot_When_loading_an_aggregate_It_should_read_the_events()
    {
        const string aggregateId = "";
        const int version = 1;
        var snapshot = new TestAggregate.TestSnapshotDto
        {
            Name = "This is a name"
        };
        AmazonServices.StsService.RespondWithDefaultAssumeRoleWithWebIdentityResponse();
        var snapshotRequestSubscription =
            AmazonServices.DynamoDb.OnGetItemRequest(DynamoDbService.ReturnSnapshot(aggregateId, version, snapshot));
        var queryEventsRequestSubscription =
            AmazonServices.DynamoDb.OnQueryRequest(DynamoDbService.ReturnEvents(aggregateId,
                new IEvent[] { new NameChanged("Name2") }));

        using var client = AmazonServices.CreateDynamoDbClient();
        var config = new EventStoreConfig();
        var eventStore = new EventStore(client, config);

        var aggregate = new TestAggregate("Test");
        await eventStore.LoadAsync(aggregate, TimeoutToken)
            .ConfigureAwait(false);

        var snapshotRequest = await snapshotRequestSubscription.ConfigureAwait(false);
        snapshotRequest.AssertGetSnapshotItemRequest(aggregate, config);

        var queryEventsRequest = await queryEventsRequestSubscription.ConfigureAwait(false);
        queryEventsRequest.AssertEventsQueried(aggregate, config, version);

        aggregate.Name.Should().Be("Name2");
    }
}
