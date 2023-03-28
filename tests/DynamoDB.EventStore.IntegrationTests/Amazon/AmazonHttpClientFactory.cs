using Amazon.Runtime;

namespace DynamoDB.EventStore.IntegrationTests.Amazon;

internal sealed class AmazonHttpClientFactory : HttpClientFactory
{
    private readonly HttpMessageHandler _messageHandler;

    public AmazonHttpClientFactory(HttpMessageHandler messageHandler)
    {
        _messageHandler = messageHandler;
    }
        
    public override HttpClient CreateHttpClient(IClientConfig clientConfig) => 
        new(_messageHandler);
}