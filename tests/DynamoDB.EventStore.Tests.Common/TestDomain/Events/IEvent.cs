using System.Text.Json.Serialization;

namespace DynamoDB.EventStore.Tests.Common.TestDomain.Events;

[JsonDerivedType(typeof(NameChanged), typeDiscriminator: nameof(NameChanged))]
public interface IEvent
{
}