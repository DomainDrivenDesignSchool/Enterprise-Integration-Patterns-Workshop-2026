using System.Text.Json;
using System.Text.Json.Nodes;

namespace Workshop.EventStore;

public interface IEventUpcaster
{
    string EventType { get; }
    int FromVersion { get; }
    int ToVersion { get; }
    JsonNode Upcast(JsonNode payload);
}

/// <summary>Applies one-version-at-a-time transformations. Missing links fail loudly; events are never guessed.</summary>
public sealed class EventUpcasterRegistry
{
    private readonly Dictionary<(string EventType, int FromVersion), IEventUpcaster> _upcasters = [];

    public EventUpcasterRegistry Register(IEventUpcaster upcaster)
    {
        ArgumentNullException.ThrowIfNull(upcaster);

        if (string.IsNullOrWhiteSpace(upcaster.EventType))
            throw new ArgumentException("Event type is required.", nameof(upcaster));

        if (upcaster.FromVersion <= 0 || upcaster.ToVersion != upcaster.FromVersion + 1)
            throw new ArgumentException("Upcasters must advance exactly one schema version.", nameof(upcaster));
        
        if (!_upcasters.TryAdd((upcaster.EventType, upcaster.FromVersion), upcaster))
            throw new InvalidOperationException($"An upcaster for {upcaster.EventType} v{upcaster.FromVersion} is already registered.");
       
        return this;
    }

    public (int Version, JsonElement Payload) Upcast(string eventType, int sourceVersion, JsonElement payload)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventType);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(sourceVersion);

        var currentVersion = sourceVersion;
        var current = JsonNode.Parse(payload.GetRawText())
            ?? throw new JsonException("Event payload cannot be JSON null.");

        var visited = new HashSet<int>();

        while (_upcasters.TryGetValue((eventType, currentVersion), out var upcaster))
        {
            if (!visited.Add(currentVersion))
                throw new InvalidOperationException($"Upcaster cycle detected for {eventType}.");
            
            current = upcaster.Upcast(current)
                ?? throw new InvalidOperationException($"Upcaster {eventType} v{currentVersion} returned null.");
            
            currentVersion = upcaster.ToVersion;
        }

        // A gap is detectable when a later registered version exists but the current link is absent.
        if (_upcasters.Keys.Any(x => x.EventType == eventType && x.FromVersion > currentVersion))
            throw new InvalidOperationException($"Missing upcaster for {eventType} v{currentVersion}.");

        return (currentVersion, JsonSerializer.SerializeToElement(current));
    }
}
