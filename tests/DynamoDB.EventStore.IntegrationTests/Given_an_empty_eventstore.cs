using Amazon.DynamoDBv2;
using DynamoDB.EventStore.IntegrationTests.Amazon;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB;
using FluentAssertions;

namespace DynamoDB.EventStore.IntegrationTests
{
    public class Given_an_empty_eventstore
    {
        [Fact]
        public async Task When_loading_an_aggregate_It_should_load_the_aggregate()
        {
            var amazonServices = new AmazonServices();
            amazonServices.StsService.RespondWithDefaultAssumeRoleWithWebIdentityResponse();
            var snapshotRequestSubscription = amazonServices.DynamoDb.OnGetItemRequest(DynamoDbService.ReturnEmptyGetItemResponse);
            var querySubscription = amazonServices.DynamoDb.OnQueryRequest(DynamoDbService.ReturnEmptyQueryResponse);

            using var client = new AmazonDynamoDBClient(
                new ConfigurableAssumeRoleWithWebIdentityCredentials(amazonServices.HttpClientFactory),
                new AmazonDynamoDBConfig
                {
                    HttpClientFactory = amazonServices.HttpClientFactory
                });
            var config = new EventStoreConfig();
            var eventStore = new EventStore(client, config);

            var aggregate = new TestDomain.TestAggregate("Test");
            await eventStore.LoadAsync(aggregate)
                .ConfigureAwait(false);

            var snapshotRequest = await snapshotRequestSubscription.ConfigureAwait(false);
            snapshotRequest.Should().NotBeNull();
            snapshotRequest!.Key.Should().ContainKey("PK").WhoseValue.S.Should().Be("Test");
            snapshotRequest.Key.Should().ContainKey("SK").WhoseValue.S.Should().Be("S");

            var queryRequest = await querySubscription.ConfigureAwait(false);
            queryRequest.Should().NotBeNull();
            queryRequest!.TableName.Should().Be(config.TableName);
            queryRequest.ConsistentRead.Should().Be(config.ConsistentRead);
            
            aggregate.Name.Should().BeNull();
        }
    }
}
