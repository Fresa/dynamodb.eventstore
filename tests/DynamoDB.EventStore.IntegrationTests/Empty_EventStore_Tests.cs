using Amazon.DynamoDBv2.Model;
using DynamoDB.EventStore.IntegrationTests.Amazon;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;
using DynamoDB.EventStore.IntegrationTests.TestDomain;
using DynamoDB.EventStore.IntegrationTests.TestDomain.Assertion;
using DynamoDB.EventStore.IntegrationTests.TestDomain.Commands;

namespace DynamoDB.EventStore.IntegrationTests;

public class Given_an_empty_eventstore
{
    [Fact]
    public async Task When_loading_an_aggregate_It_should_load_the_aggregate()
    {
        var amazonServices = new AmazonServices();
        amazonServices.StsService.RespondWithDefaultAssumeRoleWithWebIdentityResponse();
        var snapshotRequestSubscription = amazonServices.DynamoDb.OnGetItemRequest(DynamoDbService.ReturnEmptyGetItemResponse);
        var queryEventsRequestSubscription = amazonServices.DynamoDb.OnQueryRequest(DynamoDbService.ReturnEmptyQueryResponse);

        using var client = amazonServices.CreateDynamoDbClient();
        var config = new EventStoreConfig();
        var eventStore = new EventStore(client, config);

        var aggregate = new TestAggregate("Test");
        await eventStore.LoadAsync(aggregate)
            .ConfigureAwait(false);

        var snapshotRequest = await snapshotRequestSubscription.ConfigureAwait(false);
        snapshotRequest.Should().NotBeNull();
        snapshotRequest!.AssertGetSnapshotItemRequest(aggregate, config);

        var queryEventsRequest = await queryEventsRequestSubscription.ConfigureAwait(false);
        queryEventsRequest.Should().NotBeNull();
        queryEventsRequest!.AssertEventsQueried(aggregate, config);
            
        aggregate.Name.Should().BeNull();
    }

    [Fact]
    public async Task When_saving_events_The_events_should_be_stored()
    {
        var amazonServices = new AmazonServices();
        amazonServices.StsService.RespondWithDefaultAssumeRoleWithWebIdentityResponse();

        List<UpdateItemRequest?> updateRequests = new();
        amazonServices.DynamoDb.OnUpdateItemRequest(async (request, cancellation) =>
        {
            var updateItemRequest = await request.Content
                .ReadAmazonDynamoDbRequestFromJsonAsync<UpdateItemRequest>(cancellation)
                .ConfigureAwait(false);
            updateRequests.Add(updateItemRequest);

            return DynamoDbService.CreateEmptyResponse();
        });

        using var client = amazonServices.CreateDynamoDbClient();
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
        eventsAdded.Should().NotBeNull();
        eventsAdded!.AssertEventsAdded(aggregate, config, uncommittedEvents);

        var updateSnapshotRequest = updateRequests[1];
        updateSnapshotRequest.Should().NotBeNull();
        await updateSnapshotRequest!.AssertSnapshotUpdatedAsync(aggregate, config)
            .ConfigureAwait(false);
    }
}