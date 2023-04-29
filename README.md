# DynamoDB Eventstore
An event store implementation using Amazon DynamoDB. 

More details around the implementation can be found in this [article](https://blog.fkan.se/net/dynamodb-as-an-event-store/).

[![Continuous Delivery](https://github.com/Fresa/dynamodb.eventstore/actions/workflows/cd.yml/badge.svg)](https://github.com/Fresa/dynamodb.eventstore/actions/workflows/cd.yml)

## Installation
```Shell
dotnet add package DynamoDB.EventStore
```

https://www.nuget.org/packages/DynamoDB.EventStore/

## Getting Started
Create the DynamoDB table according to how you [configure the event store](src/DynamoDB.EventStore/EventStoreConfig.cs). A programatic example can be found [here](tests/DynamoDB.EventStore.SystemTests/Amazon/DynamoDbClientExtensions.cs). Make sure to give the DynamoDB client enough [permissions](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazondynamodb.html#amazondynamodb-actions-as-permissions). The event store uses the [GetItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html), [UpdateItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html) and [Query](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) actions.

The following is a minimal example on how to integrate with the event store.
```c#
using Amazon.DynamoDBv2;
using DynamoDB.EventStore;

namespace MyEventStore;

public class MyAggregate : Aggregate
{
    public MyAggregate(string id) : base(id)
    {
    }

    protected override Task<MemoryStream> CreateSnapShotAsync(CancellationToken cancellationToken)
    {
        // Make sure not to dispose the stream as it will be used to send the snapshot to DynamoDB. The event store will dispose the stream after it has been sent.
        var stream = new MemoryStream();
        // todo: Snapshot the aggregate state here and write it to the stream.
        return Task.FromResult(stream);
    }

    protected override Task LoadSnapshotAsync(MemoryStream snapshot, CancellationToken cancellationToken)
    {
        // todo: Load the aggregate state from the snapshot here
        return Task.CompletedTask;
    }

    protected override Task LoadEventsAsync(IEnumerable<MemoryStream> events, CancellationToken cancellationToken)
    {
        // todo: Load the events here
        return Task.CompletedTask;
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        using var client = new AmazonDynamoDBClient();
        var config = new EventStoreConfig();
        var eventStore = new EventStore(client, config);

        var aggregate = new MyAggregate("my-aggregate-1");
        await eventStore.LoadAsync(aggregate);
        
        // todo: Apply some changes to the aggregate here

        await eventStore.SaveAsync(aggregate);
    }
}
```

For more examples, see the [integration-](tests/DynamoDB.EventStore.IntegrationTests/EventStore_Tests.cs) and [system tests](tests/DynamoDB.EventStore.SystemTests/EventStore_Tests.cs).

# Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

# License
[MIT](LICENSE)