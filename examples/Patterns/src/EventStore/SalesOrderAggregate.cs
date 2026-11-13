using System.Text.Json;

namespace Workshop.EventStore;

/// <summary>Small reference aggregate used to teach append, replay and snapshot semantics.</summary>
public sealed class SalesOrderAggregate : IEventSourcedAggregate
{
    public Guid Id { get; private set; }
    public long Version { get; private set; }
    public Guid CustomerId { get; private set; }
    public string Currency { get; private set; } = "";
    public bool IsConfirmed { get; private set; }
    public List<SalesOrderLineAdded> Lines { get; } = [];
    public decimal Total => Lines.Sum(x => x.Quantity * x.UnitPrice);

    public static IReadOnlyList<NewDomainEvent> Create(Guid customerId, string currency)
    {
        if (customerId == Guid.Empty) throw new ArgumentException("Customer id is required.", nameof(customerId));
        ArgumentException.ThrowIfNullOrWhiteSpace(currency);
        return [NewDomainEvent.Create("SalesOrderCreated", 1, new SalesOrderCreated(customerId, currency))];
    }

    public void AddLine(string sku, int quantity, decimal unitPrice)
    {
        if (Id == Guid.Empty) throw new InvalidOperationException("Order has not been created.");
        if (IsConfirmed || PendingEvents.Any(x => x.EventType == "SalesOrderConfirmed"))
            throw new InvalidOperationException("Confirmed orders are immutable.");
        ArgumentException.ThrowIfNullOrWhiteSpace(sku);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(quantity);
        ArgumentOutOfRangeException.ThrowIfNegative(unitPrice);
        // Commands produce events; persistence is performed by IEventStore.AppendAsync with this aggregate version.
        PendingEvents.Add(NewDomainEvent.Create("SalesOrderLineAdded", 1, new SalesOrderLineAdded(sku, quantity, unitPrice)));
    }

    public void Confirm()
    {
        var hasLines = Lines.Count > 0 || PendingEvents.Any(x => x.EventType == "SalesOrderLineAdded");
        if (Id == Guid.Empty || IsConfirmed || PendingEvents.Any(x => x.EventType == "SalesOrderConfirmed") || !hasLines)
            throw new InvalidOperationException("Order cannot be confirmed in current state.");
        PendingEvents.Add(NewDomainEvent.Create("SalesOrderConfirmed", 1,
            new SalesOrderConfirmed(DateTimeOffset.UtcNow)));
    }

    public List<NewDomainEvent> PendingEvents { get; } = [];

    /// <summary>Call only after the Event Store append commits successfully.</summary>
    public void ApplyCommitted(IReadOnlyList<StoredDomainEvent> committed)
    {
        if (committed.Count != PendingEvents.Count ||
            !committed.Select(x => x.EventId).SequenceEqual(PendingEvents.Select(x => x.EventId)))
            throw new InvalidOperationException("Committed events do not match this aggregate's pending events.");
        foreach (var domainEvent in committed) Apply(domainEvent);
        PendingEvents.Clear();
    }

    public void Apply(StoredDomainEvent domainEvent)
    {
        ArgumentNullException.ThrowIfNull(domainEvent);
        if (domainEvent.AggregateId == Guid.Empty)
            throw new InvalidDataException("Event aggregate id is required.");
        if (domainEvent.AggregateId != Id && Id != Guid.Empty)
            throw new InvalidOperationException("Event belongs to a different aggregate.");
        if (domainEvent.Version != Version + 1)
            throw new InvalidDataException($"Expected event version {Version + 1}, got {domainEvent.Version}.");

        switch (domainEvent.EventType)
        {
            case "SalesOrderCreated":
                if (Id != Guid.Empty) throw new InvalidOperationException("Order is already created.");
                var created = domainEvent.Payload.Deserialize<SalesOrderCreated>()!;
                if (created.CustomerId == Guid.Empty || string.IsNullOrWhiteSpace(created.Currency))
                    throw new InvalidDataException("SalesOrderCreated contains invalid customer or currency data.");
                Id = domainEvent.AggregateId;
                CustomerId = created.CustomerId;
                Currency = created.Currency;
                break;
            case "SalesOrderLineAdded":
                if (Id == Guid.Empty || IsConfirmed) throw new InvalidOperationException("Cannot add line in current state.");
                var line = domainEvent.Payload.Deserialize<SalesOrderLineAdded>()!;
                if (string.IsNullOrWhiteSpace(line.Sku) || line.Quantity <= 0 || line.UnitPrice < 0)
                    throw new InvalidDataException("SalesOrderLineAdded contains invalid line data.");
                Lines.Add(line);
                break;
            case "SalesOrderConfirmed":
                if (Id == Guid.Empty || IsConfirmed || Lines.Count == 0)
                    throw new InvalidOperationException("Order cannot be confirmed in current state.");
                _ = domainEvent.Payload.Deserialize<SalesOrderConfirmed>()
                    ?? throw new InvalidDataException("SalesOrderConfirmed payload is invalid.");
                IsConfirmed = true;
                break;
            default:
                throw new NotSupportedException($"Unknown event type '{domainEvent.EventType}'.");
        }
        Version = domainEvent.Version;
    }

    public void RestoreSnapshot(JsonElement state, long version)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);
        var snapshot = state.Deserialize<SalesOrderSnapshot>() ?? throw new JsonException("Invalid sales order snapshot.");
        if (snapshot.Id == Guid.Empty || snapshot.CustomerId == Guid.Empty ||
            string.IsNullOrWhiteSpace(snapshot.Currency) || snapshot.Lines is null)
            throw new JsonException("Snapshot aggregate state is incomplete.");
        Id = snapshot.Id;
        CustomerId = snapshot.CustomerId;
        Currency = snapshot.Currency;
        IsConfirmed = snapshot.IsConfirmed;
        Lines.Clear();
        Lines.AddRange(snapshot.Lines);
        Version = version;
        PendingEvents.Clear();
    }

    public string CaptureSnapshot()
    {
        if (Id == Guid.Empty || Version == 0) throw new InvalidOperationException("Cannot snapshot an uninitialized aggregate.");
        return JsonSerializer.Serialize(new SalesOrderSnapshot(Id, CustomerId, Currency, IsConfirmed, Lines.ToArray()));
    }

    private sealed record SalesOrderSnapshot(Guid Id, Guid CustomerId, string Currency, bool IsConfirmed,
        IReadOnlyList<SalesOrderLineAdded> Lines);
}
