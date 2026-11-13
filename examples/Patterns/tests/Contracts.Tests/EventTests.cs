using System.Text.Json;
using Workshop.Contracts;
using Xunit;
public class EventTests {
 [Fact] public void Envelope_round_trips(){var e=new IntegrationEvent<OrderCreated>(Guid.NewGuid(),"OrderCreated",1,DateTimeOffset.UtcNow,"test",Guid.NewGuid(),null,new(Guid.NewGuid(),Guid.NewGuid(),123m,"IRR"));var x=JsonSerializer.Deserialize<IntegrationEvent<OrderCreated>>(JsonSerializer.Serialize(e));Assert.NotNull(x);Assert.Equal(e.EventId,x.EventId);Assert.Equal(e.Payload.OrderId,x.Payload.OrderId);}
}
