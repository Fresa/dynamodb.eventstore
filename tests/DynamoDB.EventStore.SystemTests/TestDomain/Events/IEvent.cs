using System.Text.Json.Serialization;

namespace DynamoDB.EventStore.SystemTests.TestDomain.Events;

[JsonDerivedType(typeof(NameChanged), typeDiscriminator: nameof(NameChanged))]
internal interface IEvent
{
}