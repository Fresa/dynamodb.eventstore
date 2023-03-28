using System.Text.Json.Serialization;

namespace DynamoDB.EventStore.IntegrationTests.TestDomain.Events;

[JsonDerivedType(typeof(NameChanged), typeDiscriminator: nameof(NameChanged))]
internal interface IEvent
{
}