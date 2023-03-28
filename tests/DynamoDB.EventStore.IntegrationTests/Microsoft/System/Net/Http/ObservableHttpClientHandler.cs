using System.Collections.Concurrent;
using System.Text;

namespace DynamoDB.EventStore.IntegrationTests.Microsoft.System.Net.Http;

internal abstract class ObservableHttpClientHandler
{
    private readonly ConcurrentDictionary<string, OnReceiveAsyncHandler> _subscriptions = new();

    internal delegate Task<HttpResponseMessage> OnReceiveAsyncHandler(HttpRequestMessage request, CancellationToken
        cancellation);

    protected virtual string MapRequestToHandlerId(HttpRequestMessage message) =>
        (message.RequestUri ?? throw new NullReferenceException("Request URI is null")).ToString();

    internal void On(string handlerId, OnReceiveAsyncHandler handler)
    {
        _subscriptions.AddOrUpdate(handlerId, _ => handler, (_, _) => handler);
    }

    protected internal Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var id = MapRequestToHandlerId(request);
        if (!_subscriptions.TryGetValue(id, out var handler))
        {
            throw new InvalidOperationException($"""
                Received request with id {id} which has not been subscribed.
                Subscriptions:
                {_subscriptions.Aggregate(new StringBuilder(), (message, subscription) => message.AppendLine($"  {subscription.Key}"))}
                """
            );
        }

        return handler.Invoke(request, cancellationToken);
    }
}