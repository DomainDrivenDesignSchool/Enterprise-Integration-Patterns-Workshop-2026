using System.Text.Json;
using System.Text.Json.Nodes;
using Workshop.EventStore;
using Xunit;

public sealed class EventUpcasterRegistryTests
{
    [Fact]
    public void AppliesEveryVersionInOrder()
    {
        var registry = new EventUpcasterRegistry()
            .Register(new RenameAmountV1ToV2())
            .Register(new AddCurrencyV2ToV3());
        using var document = JsonDocument.Parse("{\"amount\":12.5}");

        var (version, payload) = registry.Upcast("PaymentCaptured", 1, document.RootElement);

        Assert.Equal(3, version);
        Assert.Equal(12.5m, payload.GetProperty("total").GetDecimal());
        Assert.Equal("IRR", payload.GetProperty("currency").GetString());
    }

    [Fact]
    public void RejectsVersionGapInsteadOfSilentlyPassingOldPayload()
    {
        var registry = new EventUpcasterRegistry().Register(new AddCurrencyV2ToV3());
        using var document = JsonDocument.Parse("{\"amount\":12.5}");
        Assert.Throws<InvalidOperationException>(() => registry.Upcast("PaymentCaptured", 1, document.RootElement));
    }

    [Fact]
    public void RejectsDuplicateUpcasterRegistration()
    {
        var registry = new EventUpcasterRegistry().Register(new RenameAmountV1ToV2());
        Assert.Throws<InvalidOperationException>(() => registry.Register(new RenameAmountV1ToV2()));
    }

    private sealed class RenameAmountV1ToV2 : IEventUpcaster
    {
        public string EventType => "PaymentCaptured";
        public int FromVersion => 1;
        public int ToVersion => 2;
        public JsonNode Upcast(JsonNode payload)
        {
            var amount = payload["amount"]!.GetValue<decimal>();
            return new JsonObject { ["total"] = amount };
        }
    }

    private sealed class AddCurrencyV2ToV3 : IEventUpcaster
    {
        public string EventType => "PaymentCaptured";
        public int FromVersion => 2;
        public int ToVersion => 3;
        public JsonNode Upcast(JsonNode payload)
        {
            payload["currency"] = "IRR";
            return payload;
        }
    }
}
