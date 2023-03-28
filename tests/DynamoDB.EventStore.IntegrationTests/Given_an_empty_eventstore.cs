using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using DynamoDB.EventStore.IntegrationTests.Amazon;
using DynamoDB.EventStore.IntegrationTests.Amazon.DynamoDB.Serialization;
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
            GetItemRequest? snapshotRequest = default;
            amazonServices.DynamoDb.On(amazonServices.DynamoDb.GetItemKey, async (request, cancellation) =>
            {
                snapshotRequest = await request.Content.ReadGetItemRequestFromJsonAsync(cancellation)
                    .ConfigureAwait(false);

                return new HttpResponseMessage
                {
                    Content = new StringContent("""
                {
                    "Attributes": {
                        
                    }
                }
                """)
                };
            });

            QueryRequest? queryRequest = default;
            amazonServices.DynamoDb.On(amazonServices.DynamoDb.QueryKey, async (request, cancellation) =>
            {
                queryRequest = await request.Content.ReadQueryRequestFromJsonAsync(cancellation)
                    .ConfigureAwait(false);
                return new HttpResponseMessage
                {
                    Content = new StringContent("""
                {
                    "Attributes": {
                        
                    }
                }
                """)
                };
            });

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

            snapshotRequest.Should().NotBeNull();
            snapshotRequest!.Key.Should().ContainKey("PK").WhoseValue.S.Should().Be("Test");
            snapshotRequest.Key.Should().ContainKey("SK").WhoseValue.S.Should().Be("S");

            queryRequest.Should().NotBeNull();
            queryRequest!.TableName.Should().Be(config.TableName);
            queryRequest.ConsistentRead.Should().Be(config.ConsistentRead);
            
            aggregate.Name.Should().BeNull();
        }
    }
}